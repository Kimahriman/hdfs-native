#!/usr/bin/env bash
# Need main branch until next release for var support in dynamic loading
cargo install --git https://github.com/rust-lang/rust-bindgen --branch main bindgen-cli

bindgen c_src/gssapi_mit.h \
    --allowlist-type "OM_.+|gss_.+" \
    --allowlist-var "_?GSS_.+|gss_.+" \
    --allowlist-function "gss_.*" \
    --dynamic-loading GSSAPI \
    -o src/security/gssapi_bindings.rs