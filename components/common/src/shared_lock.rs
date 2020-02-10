use std::sync::{RwLock, Arc};

pub type SharedRwLock<T> = Arc<RwLock<T>>;

pub fn new_shared_rw_lock<T>(t: T) -> SharedRwLock<T> {
    Arc::new(RwLock::new(t))
}

pub type SharedRwLockVec<T> = SharedRwLock<Vec<T>>;

pub fn new_shared_rw_lock_vec<T>() -> SharedRwLockVec<T> {
    new_shared_rw_lock(Vec::new())
}