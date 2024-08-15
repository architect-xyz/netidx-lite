use crate::pool::{Pool, Poolable, Pooled};
use fxhash::FxHashMap;
use std::{
    any::{Any, TypeId},
    cell::RefCell,
    collections::HashMap,
};

#[macro_export]
macro_rules! try_continue {
    ($msg:expr, $e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => {
                log::error!("{}: {:?}", $msg, e);
                continue;
            }
        }
    };
}

thread_local! {
    static POOLS: RefCell<FxHashMap<TypeId, Box<dyn Any>>> =
        RefCell::new(HashMap::default());
}

/// Take a poolable type T from the generic thread local pool set.
/// Note it is much more efficient to construct your own pools.
/// size and max are the pool parameters used if the pool doesn't
/// already exist.
pub fn take_t<T: Any + Poolable + Send + 'static>(size: usize, max: usize) -> Pooled<T> {
    POOLS.with(|pools| {
        let mut pools = pools.borrow_mut();
        let pool: &mut Pool<T> = pools
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Box::new(Pool::<T>::new(size, max)))
            .downcast_mut()
            .unwrap();
        pool.take()
    })
}
