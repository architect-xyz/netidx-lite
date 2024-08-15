use crate::{
    pack::{Pack, PackError},
    pool::{Pool, Poolable, Pooled},
};
use anyhow::Result;
use bytes::BytesMut;
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

#[macro_export]
macro_rules! try_break {
    ($lbl:tt, $e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => {
                break $lbl Err(Error::from(e));
            }
        }
    };
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => {
                break Err(Error::from(e));
            }
        }
    };
}

thread_local! {
    static BUF: RefCell<BytesMut> = RefCell::new(BytesMut::with_capacity(512));
}

/// pack T and return a bytesmut from the global thread local buffer
pub fn pack<T: Pack>(t: &T) -> Result<BytesMut, PackError> {
    BUF.with(|buf| {
        let mut b = buf.borrow_mut();
        t.encode(&mut *b)?;
        Ok(b.split())
    })
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
