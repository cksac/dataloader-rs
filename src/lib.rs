extern crate futures;
extern crate tokio_core;

use futures::Future;

pub mod non_cached;
pub use non_cached::*;

pub mod cached;

#[cfg(test)]
mod tests;

#[derive(Clone, PartialEq, Debug)]
pub enum LoadError<E> {
    SenderDropped,
    UnequalKeyValueSize {
        key_count: usize,
        value_count: usize,
    },
    BatchFn(E),
}

pub type BatchFuture<V, E> = Box<Future<Item = Vec<V>, Error = E>>;

pub trait BatchFn<K, V> {
    type Error;

    fn load(&self, keys: &[K]) -> BatchFuture<V, Self::Error>;

    fn max_batch_size(&self) -> usize {
        200
    }
}