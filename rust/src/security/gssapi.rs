use core::fmt;
use log::warn;
use once_cell::sync::Lazy;
use std::marker::PhantomData;
use std::ops::Deref;
use std::os::raw::c_void;
use std::slice::from_raw_parts;
use std::{ptr, slice};

use crate::HdfsError;

use super::sasl::SaslSession;
use super::user::User;

mod bindings {
    #![allow(warnings)]
    include!("./gssapi_bindings.rs");

    unsafe impl Send for GSSAPI {}
    unsafe impl Sync for GSSAPI {}
}

// Converting error codes to a bitflag provides names in debug output
bitflags::bitflags! {
    #[derive(Clone, Copy, Debug)]
    pub struct GssMajorCodes: u32 {
        const GSS_S_CALL_INACCESSIBLE_READ = bindings::_GSS_S_CALL_INACCESSIBLE_READ;
        const GSS_S_CALL_INACCESSIBLE_WRITE = bindings::_GSS_S_CALL_INACCESSIBLE_WRITE;
        const GSS_S_CALL_BAD_STRUCTURE = bindings::_GSS_S_CALL_BAD_STRUCTURE;
        const GSS_S_BAD_MECH = bindings::_GSS_S_BAD_MECH;
        const GSS_S_BAD_NAME = bindings::_GSS_S_BAD_NAME;
        const GSS_S_BAD_NAMETYPE = bindings::_GSS_S_BAD_NAMETYPE;
        const GSS_S_BAD_BINDINGS = bindings::_GSS_S_BAD_BINDINGS;
        const GSS_S_BAD_STATUS = bindings::_GSS_S_BAD_STATUS;
        const GSS_S_BAD_SIG = bindings::_GSS_S_BAD_SIG;
        const GSS_S_BAD_MIC = bindings::_GSS_S_BAD_MIC;
        const GSS_S_NO_CRED = bindings::_GSS_S_NO_CRED;
        const GSS_S_NO_CONTEXT = bindings::_GSS_S_NO_CONTEXT;
        const GSS_S_DEFECTIVE_TOKEN = bindings::_GSS_S_DEFECTIVE_TOKEN;
        const GSS_S_DEFECTIVE_CREDENTIAL = bindings::_GSS_S_DEFECTIVE_CREDENTIAL;
        const GSS_S_CREDENTIALS_EXPIRED = bindings::_GSS_S_CREDENTIALS_EXPIRED;
        const GSS_S_CONTEXT_EXPIRED = bindings::_GSS_S_CONTEXT_EXPIRED;
        const GSS_S_FAILURE = bindings::_GSS_S_FAILURE;
        const GSS_S_BAD_QOP = bindings::_GSS_S_BAD_QOP;
        const GSS_S_UNAUTHORIZED = bindings::_GSS_S_UNAUTHORIZED;
        const GSS_S_UNAVAILABLE = bindings::_GSS_S_UNAVAILABLE;
        const GSS_S_DUPLICATE_ELEMENT = bindings::_GSS_S_DUPLICATE_ELEMENT;
        const GSS_S_NAME_NOT_MN = bindings::_GSS_S_NAME_NOT_MN;
        const GSS_S_CONTINUE_NEEDED = bindings::_GSS_S_CONTINUE_NEEDED;
        const GSS_S_DUPLICATE_TOKEN = bindings::_GSS_S_DUPLICATE_TOKEN;
        const GSS_S_OLD_TOKEN = bindings::_GSS_S_OLD_TOKEN;
        const GSS_S_UNSEQ_TOKEN = bindings::_GSS_S_UNSEQ_TOKEN;
        const GSS_S_GAP_TOKEN = bindings::_GSS_S_GAP_TOKEN;
    }
}

static LIBGSSAPI: Lazy<Option<bindings::GSSAPI>> = Lazy::new(|| {
    // Debian systems don't have a symlink for just libgssapi_krb5.so, only libgssapi_krb5.so.2
    // RHEL based systems have this .2 link also, so just use that
    #[cfg(target_os = "linux")]
    let library_name = "libgssapi_krb5.so.2";

    #[cfg(not(target_os = "linux"))]
    let library_name = libloading::library_filename("gssapi_krb5");

    match unsafe { bindings::GSSAPI::new(library_name) } {
        Ok(gssapi) => Some(gssapi),
        Err(e) => {
            #[cfg(target_os = "macos")]
            let message = "Try installing via \"brew install krb5\"";
            #[cfg(target_os = "linux")]
                let message = "On Debian based systems, try \"apt-get install libgssapi-krb5-2\". On RHEL based systems, try \"yum install krb5-libs\"";
            #[cfg(not(any(target_os = "macos", target_os = "linux")))]
            let message = "Loading Kerberos libraries are not supported on this system";
            log::warn!("Failed to libgssapi_krb5.\n{}.\n{:?}", message, e);
            None
        }
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

impl GssBuf<'_> {
    fn new() -> Self {
        Self(
            bindings::gss_buffer_desc_struct {
                length: 0,
                value: ptr::null_mut(),
            },
            PhantomData {},
        )
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
        let mut minor = bindings::GSS_S_COMPLETE;
        let mut name = ptr::null_mut::<bindings::gss_name_struct>();

        let mut name_buf = GssBuf::from(target_name);

        let major = unsafe {
            libgssapi()?.gss_import_name(
                &mut minor,
                name_buf.as_ptr(),
                *libgssapi()?.GSS_C_NT_HOSTBASED_SERVICE(),
                &mut name as *mut bindings::gss_name_t,
            )
        };
        check_gss_ok(major, minor)?;
        Ok(Self { name })
    }

    fn display_name(&self) -> crate::Result<String> {
        let mut minor = 0;
        let mut display_name = bindings::gss_buffer_desc_struct {
            length: 0,
            value: ptr::null_mut(),
        };
        let major = unsafe {
            libgssapi()?.gss_display_name(&mut minor, self.name, &mut display_name, ptr::null_mut())
        };
        check_gss_ok(major, minor)?;

        unsafe {
            if display_name.value.is_null() {
                Ok(String::new())
            } else {
                let slice: &[u8] = from_raw_parts(display_name.value.cast(), display_name.length);
                Ok(String::from_utf8_lossy(slice).to_string())
            }
        }
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

struct GssClientCtx {
    ctx: bindings::gss_ctx_id_t,
    target: GssName,
    flags: u32,
}

unsafe impl Send for GssClientCtx {}
unsafe impl Sync for GssClientCtx {}

impl GssClientCtx {
    fn new(target: GssName) -> Self {
        let flags = bindings::GSS_C_DELEG_FLAG
            | bindings::GSS_C_MUTUAL_FLAG
            | bindings::GSS_C_REPLAY_FLAG
            | bindings::GSS_C_SEQUENCE_FLAG
            | bindings::GSS_C_CONF_FLAG
            | bindings::GSS_C_INTEG_FLAG
            | bindings::GSS_C_ANON_FLAG
            | bindings::GSS_C_PROT_READY_FLAG
            | bindings::GSS_C_TRANS_FLAG
            | bindings::GSS_C_DELEG_POLICY_FLAG;
        Self {
            ctx: ptr::null_mut(),
            target,
            flags,
        }
    }

    fn step(&mut self, token: Option<&[u8]>) -> crate::Result<(Option<Vec<u8>>, bool)> {
        let mut minor = 0;
        let mut flags_out = 0;
        let mut out = bindings::gss_buffer_desc_struct {
            value: ptr::null_mut(),
            length: 0,
        };

        let mut token_buf = token.map(GssBuf::from);
        let token_ptr = token_buf
            .as_mut()
            .map(|t| unsafe { t.as_ptr() })
            .unwrap_or(ptr::null_mut());

        let major = unsafe {
            libgssapi()?.gss_init_sec_context(
                &mut minor,
                ptr::null_mut(),
                &mut self.ctx as *mut bindings::gss_ctx_id_t,
                self.target.name,
                *libgssapi()?.gss_mech_krb5() as bindings::gss_OID,
                self.flags,
                bindings::_GSS_C_INDEFINITE,
                ptr::null_mut(),
                token_ptr,
                ptr::null_mut(),
                &mut out,
                &mut flags_out,
                ptr::null_mut(),
            )
        };

        let complete = if major == bindings::GSS_S_CONTINUE_NEEDED {
            false
        } else {
            check_gss_ok(major, minor)?;
            true
        };

        self.flags |= flags_out;

        let out_token = unsafe {
            if out.value.is_null() {
                None
            } else {
                let slice: &[u8] = from_raw_parts(out.value.cast(), out.length);
                Some(slice.to_vec())
            }
        };

        Ok((out_token, complete))
    }

    fn wrap(&mut self, encrypt: bool, buf: &[u8]) -> crate::Result<Vec<u8>> {
        let mut minor = 0;
        let mut buf_in = GssBuf::from(buf);
        let mut buf_out = GssBuf::new();
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
        let mut buf_out = GssBuf::new();
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

fn check_gss_ok(mut major: u32, mut minor: u32) -> crate::Result<()> {
    major &= (bindings::_GSS_C_CALLING_ERROR_MASK << bindings::GSS_C_CALLING_ERROR_OFFSET)
        | (bindings::_GSS_C_ROUTINE_ERROR_MASK << bindings::GSS_C_ROUTINE_ERROR_OFFSET);
    if major == bindings::GSS_S_COMPLETE {
        Ok(())
    } else {
        let mut context = 0;
        let mut msg = GssBuf::new();
        let ret = unsafe {
            libgssapi()?.gss_display_status(
                &mut minor,
                major,
                bindings::GSS_C_GSS_CODE as i32,
                ptr::null_mut(),
                &mut context,
                msg.as_ptr(),
            )
        };

        let error_message = if ret == bindings::GSS_S_COMPLETE {
            String::from_utf8_lossy(msg.as_ref()).to_string()
        } else {
            String::new()
        };

        Err(HdfsError::GSSAPIError(
            GssMajorCodes::from_bits_retain(major),
            minor,
            error_message,
        ))
    }
}

#[derive(Debug)]
pub struct GssapiSession {
    state: GssapiState,
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
    pub(crate) fn new(service: &str, hostname: &str) -> crate::Result<Self> {
        let targ_name = format!("{service}@{hostname}");

        let target = GssName::with_target(&targ_name)?;
        let state = GssapiState::Pending(GssClientCtx::new(target));
        Ok(Self { state })
    }
}

impl SaslSession for GssapiSession {
    fn step(&mut self, token: Option<&[u8]>) -> crate::Result<(Vec<u8>, bool)> {
        match core::mem::replace(&mut self.state, GssapiState::Errored) {
            GssapiState::Pending(mut ctx) => {
                let mut ret = Vec::<u8>::new();
                let (out_token, complete) = ctx.step(token)?;
                if let Some(token) = out_token {
                    if !token.is_empty() {
                        ret = token.to_vec();
                    }
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
                Ok(User::get_user_info_from_principal(principal))
            }
            _ => Err(HdfsError::SASLError(
                "SASL session doesn't have security layer".to_string(),
            )),
        }
    }
}
