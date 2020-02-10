
pub mod shared_lock;

pub use shared_lock::SharedRwLock;
pub use shared_lock::new_shared_rw_lock;

pub use shared_lock::SharedRwLockVec;
pub use shared_lock::new_shared_rw_lock_vec;

use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_timestamp_mills() -> u128{
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_millis()
}