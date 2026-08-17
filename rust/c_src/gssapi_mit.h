#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>

// RFC 5587 credential-store extension. Declare the small subset used here
// directly because some platforms do not install gssapi_ext.h.
typedef struct gss_key_value_element_struct {
    const char *key;
    const char *value;
} gss_key_value_element_desc;

typedef struct gss_key_value_set_struct {
    size_t count;
    gss_key_value_element_desc *elements;
} gss_key_value_set_desc, *gss_key_value_set_t;
typedef const gss_key_value_set_desc *gss_const_key_value_set_t;

OM_uint32 gss_acquire_cred_from(
    OM_uint32 *, gss_name_t, OM_uint32, gss_OID_set, gss_cred_usage_t,
    gss_const_key_value_set_t, gss_cred_id_t *, gss_OID_set *, OM_uint32 *);

const OM_uint32 _GSS_C_INDEFINITE = GSS_C_INDEFINITE;
const OM_uint32 _GSS_C_CALLING_ERROR_MASK = GSS_C_CALLING_ERROR_MASK;
const OM_uint32 _GSS_C_ROUTINE_ERROR_MASK = GSS_C_ROUTINE_ERROR_MASK;
const OM_uint32 _GSS_C_SUPPLEMENTARY_MASK = GSS_C_SUPPLEMENTARY_MASK;
const OM_uint32 _GSS_S_CALL_INACCESSIBLE_READ = GSS_S_CALL_INACCESSIBLE_READ;
const OM_uint32 _GSS_S_CALL_INACCESSIBLE_WRITE = GSS_S_CALL_INACCESSIBLE_WRITE;
const OM_uint32 _GSS_S_CALL_BAD_STRUCTURE = GSS_S_CALL_BAD_STRUCTURE;
const OM_uint32 _GSS_S_BAD_MECH = GSS_S_BAD_MECH;
const OM_uint32 _GSS_S_BAD_NAME = GSS_S_BAD_NAME;
const OM_uint32 _GSS_S_BAD_NAMETYPE = GSS_S_BAD_NAMETYPE;
const OM_uint32 _GSS_S_BAD_BINDINGS = GSS_S_BAD_BINDINGS;
const OM_uint32 _GSS_S_BAD_STATUS = GSS_S_BAD_STATUS;
const OM_uint32 _GSS_S_BAD_SIG = GSS_S_BAD_SIG;
const OM_uint32 _GSS_S_BAD_MIC = GSS_S_BAD_SIG;
const OM_uint32 _GSS_S_NO_CRED = GSS_S_NO_CRED;
const OM_uint32 _GSS_S_NO_CONTEXT = GSS_S_NO_CONTEXT;
const OM_uint32 _GSS_S_DEFECTIVE_TOKEN = GSS_S_DEFECTIVE_TOKEN;
const OM_uint32 _GSS_S_DEFECTIVE_CREDENTIAL = GSS_S_DEFECTIVE_CREDENTIAL;
const OM_uint32 _GSS_S_CREDENTIALS_EXPIRED = GSS_S_CREDENTIALS_EXPIRED;
const OM_uint32 _GSS_S_CONTEXT_EXPIRED = GSS_S_CONTEXT_EXPIRED;
const OM_uint32 _GSS_S_FAILURE = GSS_S_FAILURE;
const OM_uint32 _GSS_S_BAD_QOP = GSS_S_BAD_QOP;
const OM_uint32 _GSS_S_UNAUTHORIZED = GSS_S_UNAUTHORIZED;
const OM_uint32 _GSS_S_UNAVAILABLE = GSS_S_UNAVAILABLE;
const OM_uint32 _GSS_S_DUPLICATE_ELEMENT = GSS_S_DUPLICATE_ELEMENT;
const OM_uint32 _GSS_S_NAME_NOT_MN = GSS_S_NAME_NOT_MN;
const OM_uint32 _GSS_S_CONTINUE_NEEDED = GSS_S_CONTINUE_NEEDED;
const OM_uint32 _GSS_S_DUPLICATE_TOKEN = GSS_S_DUPLICATE_TOKEN;
const OM_uint32 _GSS_S_OLD_TOKEN = GSS_S_OLD_TOKEN;
const OM_uint32 _GSS_S_UNSEQ_TOKEN = GSS_S_UNSEQ_TOKEN;
const OM_uint32 _GSS_S_GAP_TOKEN = GSS_S_GAP_TOKEN;
