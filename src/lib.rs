mod batch_fn;
pub mod cached;
pub mod non_cached;
mod runtime;

pub use batch_fn::BatchFn;

use std::{future::Future, pin::Pin};

/// A trait alias. Read as "a function which returns a pinned box containing a future"
pub trait WaitForWorkFn:
    Fn() -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> + Send + Sync + 'static
{
}

impl<T> WaitForWorkFn for T where
    T: Fn() -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> + Send + Sync + 'static
{
}

pub(crate) fn yield_fn(count: usize) -> impl WaitForWorkFn {
    move || {
        Box::pin(async move {
            // yield for other load to append request
            for _ in 0..count {
                runtime::yield_now().await;
            }
        })
    }
}
