use std::sync::Mutex;

pub static EC_FAULT_INJECTOR: Mutex<Option<EcFaultInjection>> = Mutex::new(None);
pub static WRITE_CONNECTION_FAULT_INJECTOR: Mutex<bool> = Mutex::new(false);
pub static WRITE_REPLY_FAULT_INJECTOR: Mutex<Option<usize>> = Mutex::new(None);

pub struct EcFaultInjection {
    pub fail_blocks: Vec<usize>,
}
