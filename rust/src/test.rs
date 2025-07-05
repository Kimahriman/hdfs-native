use std::sync::{atomic::AtomicBool, Mutex};

pub static DATANODE_CONNECT_FAULT_INJECTOR: AtomicBool = AtomicBool::new(false);
pub static DATANODE_READ_FAULT_INJECTOR: AtomicBool = AtomicBool::new(false);
pub static EC_FAULT_INJECTOR: Mutex<Option<EcFaultInjection>> = Mutex::new(None);
pub static WRITE_CONNECTION_FAULT_INJECTOR: AtomicBool = AtomicBool::new(false);
pub static WRITE_REPLY_FAULT_INJECTOR: Mutex<Option<usize>> = Mutex::new(None);

pub struct EcFaultInjection {
    pub fail_blocks: Vec<usize>,
}
