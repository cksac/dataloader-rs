use futures::future::BoxFuture;

pub mod non_cached;
pub use non_cached::*;

pub mod cached;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq)]
pub enum LoadError<E> {
    SenderDropped,
    UnequalKeyValueSize {
        key_count: usize,
        value_count: usize,
    },
    BatchFn(E),
}

pub type BatchFuture<I, E> = BoxFuture<'static, Result<Vec<I>, E>>;

pub trait BatchFn<K, V> {
    type Error;

    fn load(&self, keys: &[K]) -> BatchFuture<V, Self::Error>;

    #[inline(always)]
    fn max_batch_size(&self) -> usize {
        200
    }
}
