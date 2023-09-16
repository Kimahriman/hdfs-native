use std::sync::Mutex;

pub static EC_FAULT_INJECTOR: Mutex<Option<EcFaultInjection>> = Mutex::new(None);

pub struct EcFaultInjection {
    pub fail_blocks: Vec<usize>,
}
