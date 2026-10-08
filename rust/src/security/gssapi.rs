use core::fmt;
use log::warn;
use once_cell::sync::Lazy;
use std::ffi::CString;
use std::marker::PhantomData;
use std::ops::Deref;
use std::os::raw::c_void;
use std::{ptr, slice};

use crate::HdfsError;

use super::ClientAuth;
use super::sasl::SaslSession;
use super::user::User;

mod bindings {
    #![allow(warnings)]
    include!("./gssapi_bindings.rs");

    unsafe impl Send for GSSAPI {}
    unsafe impl Sync for GSSAPI {}
}

// GSS major statuses contain numeric calling and routine error fields. Only the
// supplementary information field is a set of flags.
#[derive(Clone, Copy)]
pub struct GssMajorCodes(u32);

impl GssMajorCodes {
    pub const GSS_S_FAILURE: Self = Self(bindings::_GSS_S_FAILURE);

    pub const fn from_raw(bits: u32) -> Self {
        Self(bits)
    }

    pub const fn raw(self) -> u32 {
        self.0
    }
}

impl fmt::Debug for GssMajorCodes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let calling_error =
            (self.0 >> bindings::GSS_C_CALLING_ERROR_OFFSET) & bindings::_GSS_C_CALLING_ERROR_MASK;
        let routine_error =
            (self.0 >> bindings::GSS_C_ROUTINE_ERROR_OFFSET) & bindings::_GSS_C_ROUTINE_ERROR_MASK;
        let supplementary_info = self.0 & bindings::_GSS_C_SUPPLEMENTARY_MASK;
        let raw = format!("{:#010x}", self.0);

        f.debug_struct("GssMajorCodes")
            .field("raw", &raw)
            .field("calling_error", &calling_error_name(calling_error))
            .field("routine_error", &routine_error_name(routine_error))
            .field(
                "supplementary_info",
                &format!("{:#06x}", supplementary_info),
            )
            .finish()
    }
}

fn calling_error_name(error: u32) -> &'static str {
    match error {
        0 => "none",
        1 => "GSS_S_CALL_INACCESSIBLE_READ",
        2 => "GSS_S_CALL_INACCESSIBLE_WRITE",
        3 => "GSS_S_CALL_BAD_STRUCTURE",
        _ => "unknown calling error",
    }
}

fn routine_error_name(error: u32) -> &'static str {
    match error {
        0 => "none",
        1 => "GSS_S_BAD_MECH",
        2 => "GSS_S_BAD_NAME",
        3 => "GSS_S_BAD_NAMETYPE",
        4 => "GSS_S_BAD_BINDINGS",
        5 => "GSS_S_BAD_STATUS",
        6 => "GSS_S_BAD_SIG/GSS_S_BAD_MIC",
        7 => "GSS_S_NO_CRED",
        8 => "GSS_S_NO_CONTEXT",
        9 => "GSS_S_DEFECTIVE_TOKEN",
        10 => "GSS_S_DEFECTIVE_CREDENTIAL",
        11 => "GSS_S_CREDENTIALS_EXPIRED",
        12 => "GSS_S_CONTEXT_EXPIRED",
        13 => "GSS_S_FAILURE",
        14 => "GSS_S_BAD_QOP",
        15 => "GSS_S_UNAUTHORIZED",
        16 => "GSS_S_UNAVAILABLE",
        17 => "GSS_S_DUPLICATE_ELEMENT",
        18 => "GSS_S_NAME_NOT_MN",
        _ => "unknown routine error",
    }
}

static LIBGSSAPI: Lazy<Option<bindings::GSSAPI>> = Lazy::new(|| {
    // Debian systems don't have a symlink for just libgssapi_krb5.so, only libgssapi_krb5.so.2
    // RHEL based systems have this .2 link also, so just use that
    #[cfg(target_os = "linux")]
    let library_name = "libgssapi_krb5.so.2";

    #[cfg(target_os = "windows")]
    let library_name = libloading::library_filename("gssapi64");

    #[cfg(target_os = "macos")]
    let library_name = libloading::library_filename("gssapi_krb5");

    #[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
    {
        match unsafe { bindings::GSSAPI::new(library_name) } {
            Ok(gssapi) => Some(gssapi),
            Err(e) => {
                #[cfg(target_os = "linux")]
                let message = "On Debian based systems, try \"apt-get install libgssapi-krb5-2\". On RHEL based systems, try \"yum install krb5-libs\"";
                #[cfg(target_os = "windows")]
                let message = "Install Kerberos from https://web.mit.edu/kerberos/dist/";
                #[cfg(target_os = "macos")]
                let message = "Try installing via \"brew install krb5\"";
                log::warn!("Failed to libgssapi_krb5.\n{}.\n{:?}", message, e);
                None
            }
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    {
        log::warn!("Loading Kerberos libraries is not supported on this system");
        None
    }
});

fn libgssapi() -> crate::Result<&'static bindings::GSSAPI> {
    LIBGSSAPI.as_ref().ok_or(HdfsError::OperationFailed(
        "Failed to load libgssapi_krb".to_string(),
    ))
}

#[repr(transparent)]
#[derive(Debug)]
struct GssBuf<'a>(bindings::gss_buffer_desc_struct, PhantomData<&'a [u8]>);

struct GssOwnedBuf(bindings::gss_buffer_desc_struct);

impl GssOwnedBuf {
    fn new() -> Self {
        Self(bindings::gss_buffer_desc_struct {
            length: 0,
            value: ptr::null_mut(),
        })
    }

    unsafe fn as_ptr(&mut self) -> bindings::gss_buffer_t {
        &mut self.0 as bindings::gss_buffer_t
    }
}

impl Deref for GssOwnedBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        if self.0.value.is_null() {
            &[]
        } else {
            unsafe { slice::from_raw_parts(self.0.value.cast(), self.0.length) }
        }
    }
}

impl Drop for GssOwnedBuf {
    fn drop(&mut self) {
        if self.0.value.is_null() {
            return;
        }
        let Ok(lib) = libgssapi() else {
            return;
        };
        let mut minor = bindings::GSS_S_COMPLETE;
        let major = unsafe { lib.gss_release_buffer(&mut minor, &mut self.0) };
        if let Err(e) = check_gss_ok(major, minor) {
            warn!("Failed to release GSSAPI buffer: {:?}", e);
        }
    }
}

impl Deref for GssBuf<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        if self.0.value.is_null() && self.0.length == 0 {
            &[]
        } else {
            unsafe { slice::from_raw_parts(self.0.value.cast(), self.0.length) }
        }
    }
}

impl<'a> From<&'a [u8]> for GssBuf<'a> {
    fn from(s: &[u8]) -> Self {
        let gss_buf = bindings::gss_buffer_desc_struct {
            length: s.len(),
            value: s.as_ptr() as *mut c_void,
        };
        GssBuf(gss_buf, PhantomData)
    }
}

impl<'a> From<&'a str> for GssBuf<'a> {
    fn from(s: &str) -> Self {
        let gss_buf = bindings::gss_buffer_desc_struct {
            length: s.len(),
            value: s.as_ptr() as *mut c_void,
        };
        GssBuf(gss_buf, PhantomData)
    }
}

impl GssBuf<'_> {
    pub(crate) unsafe fn as_ptr(&mut self) -> bindings::gss_buffer_t {
        &mut self.0 as bindings::gss_buffer_t
    }
}

struct GssName {
    name: bindings::gss_name_t,
}

impl GssName {
    fn new() -> Self {
        Self {
            name: ptr::null_mut(),
        }
    }

    fn with_target(target_name: &str) -> crate::Result<Self> {
        Self::import(target_name, unsafe {
            *libgssapi()?.GSS_C_NT_HOSTBASED_SERVICE()
        })
    }

    fn with_principal(principal: &str) -> crate::Result<Self> {
        if libgssapi()?.GSS_KRB5_NT_PRINCIPAL_NAME.is_err() {
            return Err(HdfsError::OperationFailed(
                "The installed GSSAPI library cannot import Kerberos principal names".to_string(),
            ));
        }
        Self::import(principal, unsafe {
            *libgssapi()?.GSS_KRB5_NT_PRINCIPAL_NAME() as bindings::gss_OID
        })
    }

    fn import(name_value: &str, name_type: bindings::gss_OID) -> crate::Result<Self> {
        let mut minor = bindings::GSS_S_COMPLETE;
        let mut name = ptr::null_mut::<bindings::gss_name_struct>();

        let mut name_buf = GssBuf::from(name_value);

        let major = unsafe {
            libgssapi()?.gss_import_name(
                &mut minor,
                name_buf.as_ptr(),
                name_type,
                &mut name as *mut bindings::gss_name_t,
            )
        };
        check_gss_ok(major, minor)?;
        Ok(Self { name })
    }

    fn display_name(&self) -> crate::Result<String> {
        let mut minor = 0;
        let mut display_name = GssOwnedBuf::new();
        let major = unsafe {
            libgssapi()?.gss_display_name(
                &mut minor,
                self.name,
                display_name.as_ptr(),
                ptr::null_mut(),
            )
        };
        check_gss_ok(major, minor)?;
        Ok(String::from_utf8_lossy(&display_name).to_string())
    }

    fn as_ptr(&mut self) -> *mut bindings::gss_name_t {
        &mut self.name
    }
}

impl Drop for GssName {
    fn drop(&mut self) {
        if !self.name.is_null() {
            let mut minor = bindings::GSS_S_COMPLETE;
            let major = unsafe {
                libgssapi()
                    .unwrap()
                    .gss_release_name(&mut minor, &mut self.name)
            };
            if let Err(e) = check_gss_ok(major, minor) {
                warn!("Failed to release GSSAPI name: {:?}", e);
            }
        }
    }
}

struct GssCred {
    cred: bindings::gss_cred_id_t,
}

impl GssCred {
    fn acquire_default() -> crate::Result<Self> {
        let mut minor = 0;
        let mut cred = ptr::null_mut();
        let major = unsafe {
            libgssapi()?.gss_acquire_cred(
                &mut minor,
                ptr::null_mut(),
                bindings::_GSS_C_INDEFINITE,
                ptr::null_mut(),
                bindings::GSS_C_INITIATE as bindings::gss_cred_usage_t,
                &mut cred,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        check_gss_ok(major, minor)?;
        Ok(Self { cred })
    }

    fn acquire(credentials: &crate::security::KerberosCredentials) -> crate::Result<Self> {
        let mut desired_name = credentials
            .principal
            .as_deref()
            .map(GssName::with_principal)
            .transpose()?;
        if credentials.keytab.is_none() && credentials.cache.is_none() {
            let mut minor = 0;
            let mut cred = ptr::null_mut();
            let major = unsafe {
                libgssapi()?.gss_acquire_cred(
                    &mut minor,
                    desired_name
                        .as_mut()
                        .map_or(ptr::null_mut(), |name| name.name),
                    bindings::_GSS_C_INDEFINITE,
                    ptr::null_mut(),
                    bindings::GSS_C_INITIATE as bindings::gss_cred_usage_t,
                    &mut cred,
                    ptr::null_mut(),
                    ptr::null_mut(),
                )
            };
            check_gss_ok(major, minor)?;
            return Ok(Self { cred });
        }
        if libgssapi()?.gss_acquire_cred_from.is_err() {
            return Err(HdfsError::OperationFailed(
                "The installed GSSAPI library does not support credential stores".to_string(),
            ));
        }
        let ccache_key = CString::new("ccache").expect("static string does not contain NUL");
        let keytab_key = CString::new("client_keytab").expect("static string does not contain NUL");
        let ccache = credentials
            .cache
            .as_deref()
            .map(|cache| {
                CString::new(cache).map_err(|_| {
                    HdfsError::InvalidArgument(
                        "Kerberos credential cache contains a NUL byte".to_string(),
                    )
                })
            })
            .transpose()?;
        let keytab = credentials
            .keytab
            .as_deref()
            .map(|keytab| {
                CString::new(keytab).map_err(|_| {
                    HdfsError::InvalidArgument(
                        "Kerberos keytab path contains a NUL byte".to_string(),
                    )
                })
            })
            .transpose()?;
        let mut elements = Vec::new();
        if let Some(ccache) = ccache.as_ref() {
            elements.push(bindings::gss_key_value_element_desc {
                key: ccache_key.as_ptr(),
                value: ccache.as_ptr(),
            });
        }
        if let Some(keytab) = keytab.as_ref() {
            elements.push(bindings::gss_key_value_element_desc {
                key: keytab_key.as_ptr(),
                value: keytab.as_ptr(),
            });
        }
        let store = bindings::gss_key_value_set_desc {
            count: elements.len() as bindings::OM_uint32,
            elements: elements.as_mut_ptr(),
        };
        let mut minor = 0;
        let mut cred = ptr::null_mut();
        let major = unsafe {
            libgssapi()?.gss_acquire_cred_from(
                &mut minor,
                desired_name
                    .as_mut()
                    .map_or(ptr::null_mut(), |name| name.name),
                bindings::_GSS_C_INDEFINITE,
                ptr::null_mut(),
                bindings::GSS_C_INITIATE as bindings::gss_cred_usage_t,
                &store,
                &mut cred,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        check_gss_ok(major, minor)?;
        Ok(Self { cred })
    }

    fn name(&self) -> crate::Result<GssName> {
        let mut minor = 0;
        let mut name = ptr::null_mut::<bindings::gss_name_struct>();
        let major = unsafe {
            libgssapi()?.gss_inquire_cred(
                &mut minor,
                self.cred,
                &mut name as *mut bindings::gss_name_t,
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        check_gss_ok(major, minor)?;
        Ok(GssName { name })
    }
}

impl Drop for GssCred {
    fn drop(&mut self) {
        if !self.cred.is_null() {
            let mut minor = bindings::GSS_S_COMPLETE;
            let major = unsafe {
                libgssapi()
                    .unwrap()
                    .gss_release_cred(&mut minor, &mut self.cred)
            };
            if let Err(e) = check_gss_ok(major, minor) {
                warn!("Failed to release GSSAPI credential: {:?}", e);
            }
        }
    }
}

// SPNEGO mechanism OID 1.3.6.1.5.5.2, encoded body bytes (no tag/length prefix).
static SPNEGO_OID_BYTES: [u8; 6] = [0x2b, 0x06, 0x01, 0x05, 0x05, 0x02];

#[derive(Clone, Copy)]
enum GssMech {
    Krb5,
    // Only constructed by `SpnegoSession`, which is gated on the `kms` feature.
    #[cfg_attr(not(feature = "kms"), allow(dead_code))]
    Spnego,
}

struct GssClientCtx {
    ctx: bindings::gss_ctx_id_t,
    target: GssName,
    mech: GssMech,
    flags: u32,
    credential: Option<GssCred>,
}

unsafe impl Send for GssClientCtx {}
unsafe impl Sync for GssClientCtx {}

impl GssClientCtx {
    fn with_credential(target: GssName, credential: Option<GssCred>) -> Self {
        Self::with_mech_and_credential(target, GssMech::Krb5, credential)
    }

    fn with_mech_and_credential(
        target: GssName,
        mech: GssMech,
        credential: Option<GssCred>,
    ) -> Self {
        // Hadoop IPC SASL uses GSSAPI/Kerberos and historically requests credential
        // delegation. SPNEGO over HTTP (KMS) does not need delegation, and the
        // delegated TGT bloats the SPNEGO token to the point where it can exceed
        // a server's default `maxHttpHeaderSize` (Tomcat defaults to 8 KiB) under
        // Active Directory PAC payloads. Use a leaner flag set for SPNEGO.
        let flags = match mech {
            GssMech::Krb5 => {
                bindings::GSS_C_DELEG_FLAG
                    | bindings::GSS_C_MUTUAL_FLAG
                    | bindings::GSS_C_REPLAY_FLAG
                    | bindings::GSS_C_SEQUENCE_FLAG
                    | bindings::GSS_C_CONF_FLAG
                    | bindings::GSS_C_INTEG_FLAG
                    | bindings::GSS_C_ANON_FLAG
                    | bindings::GSS_C_PROT_READY_FLAG
                    | bindings::GSS_C_TRANS_FLAG
                    | bindings::GSS_C_DELEG_POLICY_FLAG
            }
            GssMech::Spnego => {
                bindings::GSS_C_MUTUAL_FLAG
                    | bindings::GSS_C_REPLAY_FLAG
                    | bindings::GSS_C_SEQUENCE_FLAG
                    | bindings::GSS_C_INTEG_FLAG
            }
        };
        Self {
            ctx: ptr::null_mut(),
            target,
            mech,
            flags,
            credential,
        }
    }

    fn step(&mut self, token: Option<&[u8]>) -> crate::Result<(Option<Vec<u8>>, bool)> {
        let mut minor = 0;
        let mut flags_out = 0;
        let mut out = GssOwnedBuf::new();

        let mut token_buf = token.map(GssBuf::from);
        let token_ptr = token_buf
            .as_mut()
            .map(|t| unsafe { t.as_ptr() })
            .unwrap_or(ptr::null_mut());

        // SPNEGO needs a heap-stable gss_OID_desc whose elements point at the static OID
        // bytes. Hold it on the stack across the call.
        let mut spnego_oid_storage = bindings::gss_OID_desc {
            length: SPNEGO_OID_BYTES.len() as u32,
            elements: SPNEGO_OID_BYTES.as_ptr() as *mut c_void,
        };
        let mech_oid: bindings::gss_OID = match self.mech {
            GssMech::Krb5 => unsafe { *libgssapi()?.gss_mech_krb5() as bindings::gss_OID },
            GssMech::Spnego => &mut spnego_oid_storage as bindings::gss_OID,
        };

        let major = unsafe {
            libgssapi()?.gss_init_sec_context(
                &mut minor,
                self.credential
                    .as_ref()
                    .map(|credential| credential.cred)
                    .unwrap_or(ptr::null_mut()),
                &mut self.ctx as *mut bindings::gss_ctx_id_t,
                self.target.name,
                mech_oid,
                self.flags,
                bindings::_GSS_C_INDEFINITE,
                ptr::null_mut(),
                token_ptr,
                ptr::null_mut(),
                out.as_ptr(),
                &mut flags_out,
                ptr::null_mut(),
            )
        };

        check_gss_ok_with_mech(major, minor, mech_oid)?;
        let complete = major & bindings::GSS_S_CONTINUE_NEEDED == 0;

        self.flags |= flags_out;

        let out_token = if out.is_empty() {
            None
        } else {
            Some(out.to_vec())
        };

        Ok((out_token, complete))
    }

    fn wrap(&mut self, encrypt: bool, buf: &[u8]) -> crate::Result<Vec<u8>> {
        let mut minor = 0;
        let mut buf_in = GssBuf::from(buf);
        let mut buf_out = GssOwnedBuf::new();
        let major = unsafe {
            libgssapi()?.gss_wrap(
                &mut minor,
                self.ctx,
                if encrypt { 1 } else { 0 },
                bindings::GSS_C_QOP_DEFAULT,
                buf_in.as_ptr(),
                ptr::null_mut(),
                buf_out.as_ptr(),
            )
        };
        check_gss_ok(major, minor)?;

        Ok(buf_out.to_vec())
    }

    fn unwrap(&mut self, buf: &[u8]) -> crate::Result<Vec<u8>> {
        let mut minor = 0;
        let mut buf_in = GssBuf::from(buf);
        let mut buf_out = GssOwnedBuf::new();
        let major = unsafe {
            libgssapi()?.gss_unwrap(
                &mut minor,
                self.ctx,
                buf_in.as_ptr(),
                buf_out.as_ptr(),
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        check_gss_ok(major, minor)?;

        Ok(buf_out.to_vec())
    }

    fn source_name(&mut self) -> crate::Result<GssName> {
        let mut minor = 0;
        let mut name = GssName::new();
        let major = unsafe {
            libgssapi()?.gss_inquire_context(
                &mut minor,
                self.ctx,
                name.as_ptr(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        check_gss_ok(major, minor)?;

        Ok(name)
    }
}

impl Drop for GssClientCtx {
    fn drop(&mut self) {
        if !self.ctx.is_null() {
            let mut minor = bindings::GSS_S_COMPLETE;
            let major = unsafe {
                libgssapi().unwrap().gss_delete_sec_context(
                    &mut minor,
                    &mut self.ctx,
                    ptr::null_mut(),
                )
            };
            if let Err(e) = check_gss_ok(major, minor) {
                warn!("Failed to release GSSAPI context: {:?}", e);
            }
        }
    }
}

#[repr(u8)]
enum SecurityLayer {
    None = 1,
    Integrity = 2,
    Confidentiality = 4,
}

fn check_gss_ok(major: u32, minor: u32) -> crate::Result<()> {
    check_gss_ok_with_mech(major, minor, ptr::null_mut())
}

fn check_gss_ok_with_mech(
    major: u32,
    minor: u32,
    mech_type: bindings::gss_OID,
) -> crate::Result<()> {
    let error_mask = (bindings::_GSS_C_CALLING_ERROR_MASK << bindings::GSS_C_CALLING_ERROR_OFFSET)
        | (bindings::_GSS_C_ROUTINE_ERROR_MASK << bindings::GSS_C_ROUTINE_ERROR_OFFSET);
    if major & error_mask == 0 {
        return Ok(());
    }

    let mut error_message = display_status(major, bindings::GSS_C_GSS_CODE as i32, mech_type);
    if minor != 0 {
        let mech_message = display_status(minor, bindings::GSS_C_MECH_CODE as i32, mech_type);
        if !mech_message.is_empty() {
            if !error_message.is_empty() {
                error_message.push_str(": ");
            }
            error_message.push_str(&mech_message);
        }
    }

    Err(HdfsError::GSSAPIError(
        GssMajorCodes::from_raw(major),
        minor,
        error_message,
    ))
}

fn display_status(status_value: u32, status_type: i32, mech_type: bindings::gss_OID) -> String {
    let Ok(lib) = libgssapi() else {
        return String::new();
    };
    let mut context = 0;
    let mut messages = Vec::new();

    loop {
        let mut display_minor = 0;
        let mut msg = GssOwnedBuf::new();
        let ret = unsafe {
            lib.gss_display_status(
                &mut display_minor,
                status_value,
                status_type,
                mech_type,
                &mut context,
                msg.as_ptr(),
            )
        };
        if ret != bindings::GSS_S_COMPLETE {
            break;
        }
        if !msg.is_empty() {
            messages.push(String::from_utf8_lossy(msg.as_ref()).to_string());
        }
        if context == 0 {
            break;
        }
    }

    messages.join(": ")
}

#[derive(Debug)]
pub struct GssapiSession {
    state: GssapiState,
    effective_user: Option<String>,
}

enum GssapiState {
    Pending(GssClientCtx),
    Last(GssClientCtx),
    Completed((String, Option<(GssClientCtx, bool)>)),
    Errored,
}

impl fmt::Debug for GssapiState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending(..) => f.write_str("Pending"),
            Self::Last(..) => f.write_str("Last"),
            Self::Completed(..) => f.write_str("Completed"),
            Self::Errored => f.write_str("Errored"),
        }
    }
}

impl GssapiSession {
    pub(crate) fn new(
        service: &str,
        hostname: &str,
        effective_user: Option<String>,
        auth: Option<std::sync::Arc<ClientAuth>>,
    ) -> crate::Result<Self> {
        let targ_name = format!("{service}@{hostname}");

        let target = GssName::with_target(&targ_name)?;
        let credential = match auth.as_deref().and_then(ClientAuth::credentials) {
            Some(credentials) => Some(GssCred::acquire(credentials)?),
            None => None,
        };
        let state = GssapiState::Pending(GssClientCtx::with_credential(target, credential));
        Ok(Self {
            state,
            effective_user,
        })
    }

    pub(crate) fn get_default_principal() -> crate::Result<String> {
        let cred = GssCred::acquire_default()?;
        let name = cred.name()?;
        name.display_name()
    }
}

/// SPNEGO/Kerberos session for HTTP `Authorization: Negotiate` flows.
///
/// The first call returns the initial token to send to the server. If the server
/// replies with a continuation token (rare in Hadoop deployments), call again with
/// that token until `complete` is `true`.
#[cfg(feature = "kms")]
pub(crate) struct SpnegoSession {
    ctx: GssClientCtx,
    complete: bool,
}

#[cfg(feature = "kms")]
impl SpnegoSession {
    pub(crate) fn new(
        service: &str,
        hostname: &str,
        auth: Option<std::sync::Arc<ClientAuth>>,
    ) -> crate::Result<Self> {
        let target = GssName::with_target(&format!("{service}@{hostname}"))?;
        let credential = match auth.as_deref().and_then(ClientAuth::credentials) {
            Some(credentials) => Some(GssCred::acquire(credentials)?),
            None => None,
        };
        Ok(Self {
            ctx: GssClientCtx::with_mech_and_credential(target, GssMech::Spnego, credential),
            complete: false,
        })
    }

    /// Drive the next leg of the SPNEGO handshake. Returns the next token to send
    /// (empty if the handshake is finished). `is_complete()` is true after the
    /// final step from the GSSAPI library's perspective.
    pub(crate) fn step(&mut self, server_token: Option<&[u8]>) -> crate::Result<Vec<u8>> {
        let (out_token, complete) = self.ctx.step(server_token)?;
        self.complete = complete;
        Ok(out_token.unwrap_or_default())
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.complete
    }
}

impl SaslSession for GssapiSession {
    fn step(&mut self, token: Option<&[u8]>) -> crate::Result<(Vec<u8>, bool)> {
        match core::mem::replace(&mut self.state, GssapiState::Errored) {
            GssapiState::Pending(mut ctx) => {
                let mut ret = Vec::<u8>::new();
                let (out_token, complete) = ctx.step(token)?;
                if let Some(token) = out_token
                    && !token.is_empty()
                {
                    ret = token.to_vec();
                }
                if !complete {
                    self.state = GssapiState::Pending(ctx);
                    return Ok((ret, false));
                }

                self.state = GssapiState::Last(ctx);
                Ok((ret, false))
            }
            GssapiState::Last(mut ctx) => {
                let input = token.ok_or(HdfsError::SASLError(
                    "Token not provided during kerberos SASL negotiation".to_string(),
                ))?;
                let unwrapped = ctx.unwrap(input)?;
                if unwrapped.len() != 4 {
                    return Err(HdfsError::SASLError("Bad final token".to_string()));
                }

                let supported_sec = unwrapped[0];

                let (response, wrap) = if supported_sec & SecurityLayer::Confidentiality as u8 > 0 {
                    (
                        [SecurityLayer::Confidentiality as u8, 0xFF, 0xFF, 0xFF],
                        Some(true),
                    )
                } else if supported_sec & SecurityLayer::Integrity as u8 > 0 {
                    (
                        [SecurityLayer::Integrity as u8, 0xFF, 0xFF, 0xFF],
                        Some(false),
                    )
                } else if supported_sec & SecurityLayer::None as u8 > 0 {
                    ([SecurityLayer::None as u8, 0x00, 0x00, 0x00], None)
                } else {
                    return Err(HdfsError::SASLError(
                        "No supported security layer found".to_string(),
                    ));
                };

                let principal = ctx.source_name()?.display_name()?;

                let wrapped = ctx.wrap(false, &response)?;
                self.state = GssapiState::Completed((principal, wrap.map(|e| (ctx, e))));
                Ok((wrapped.to_vec(), true))
            }
            GssapiState::Completed(..) | GssapiState::Errored => {
                Err(HdfsError::SASLError("Mechanism done".to_string()))
            }
        }
    }

    fn has_security_layer(&self) -> bool {
        matches!(self.state, GssapiState::Completed((_, Some(_))))
    }

    fn encode(&mut self, buf: &[u8]) -> crate::Result<Vec<u8>> {
        match self.state {
            GssapiState::Completed((_, Some((ref mut ctx, encrypt)))) => {
                let wrapped = ctx.wrap(encrypt, buf)?;
                Ok(wrapped.to_vec())
            }
            _ => Err(HdfsError::SASLError(
                "SASL session doesn't have security layer".to_string(),
            )),
        }
    }

    fn decode(&mut self, buf: &[u8]) -> crate::Result<Vec<u8>> {
        match self.state {
            GssapiState::Completed((_, Some((ref mut ctx, _)))) => {
                let unwrapped = ctx.unwrap(buf)?;
                Ok(unwrapped.to_vec())
            }
            _ => Err(HdfsError::SASLError(
                "SASL session doesn't have security layer".to_string(),
            )),
        }
    }

    fn get_user_info(&self) -> crate::Result<super::user::UserInfo> {
        match &self.state {
            GssapiState::Completed((principal, _)) => {
                let user_info =
                    User::get_user_info_from_principal(principal, self.effective_user.clone());
                Ok(user_info)
            }
            _ => Err(HdfsError::SASLError(
                "SASL session doesn't have security layer".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gss_major_status_debug_decodes_routine_error_as_a_value() {
        let major = GssMajorCodes::from_raw(bindings::_GSS_S_NO_CRED);
        let debug = format!("{major:?}");

        assert!(debug.contains("routine_error: \"GSS_S_NO_CRED\""), "{debug}");
        assert!(!debug.contains("GSS_S_BAD_MECH"), "{debug}");
    }

    #[test]
    fn check_gss_ok_preserves_raw_major_and_minor_statuses() {
        if libgssapi().is_err() {
            return;
        }

        let major_code = bindings::_GSS_S_NO_CRED | bindings::GSS_S_CONTINUE_NEEDED;
        let minor_code = 0x1234_5678;
        let error = check_gss_ok(major_code, minor_code).unwrap_err();
        let HdfsError::GSSAPIError(major, minor, _) = error else {
            panic!("expected a GSSAPI error");
        };

        assert_eq!(major.raw(), major_code);
        assert_eq!(minor, minor_code);
    }
}
