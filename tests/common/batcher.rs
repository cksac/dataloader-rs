use dataloader::{BatchFn, BatchFuture};

use std::sync::atomic::{AtomicUsize, Ordering};
use futures::Future;
use futures::future::{err, ok};

pub struct Batcher {
    invoke_cnt: AtomicUsize,
    max_batch_size: usize,
}

impl Batcher {
    pub fn new(max_batch_size: usize) -> Batcher {
        Batcher {
            invoke_cnt: AtomicUsize::new(0),
            max_batch_size: max_batch_size,
        }
    }
}

impl BatchFn<i32, i32> for Batcher {
    type Error = ();
    fn load(&self, keys: &[i32]) -> BatchFuture<i32, Self::Error> {
        self.invoke_cnt.fetch_add(1, Ordering::SeqCst);
        ok(keys.into_iter().map(|v| v * 10).collect()).boxed()
    }

    fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }
}

// Result with batch call seq
impl BatchFn<i32, (usize, i32)> for Batcher {
    type Error = ();
    fn load(&self, keys: &[i32]) -> BatchFuture<(usize, i32), Self::Error> {
        let seq = self.invoke_cnt.fetch_add(1, Ordering::SeqCst);
        ok(keys.into_iter().map(|v| (seq + 1, v * 10)).collect()).boxed()
    }

    fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum MyError {
    Unknown,
}

pub struct BadBatcher;
impl BatchFn<i32, i32> for BadBatcher {
    type Error = MyError;
    fn load(&self, _keys: &[i32]) -> BatchFuture<i32, Self::Error> {
        // fail whole batch
        err(MyError::Unknown).boxed()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ValueError {
    NotEven,
}

impl BatchFn<i32, Result<i32, ValueError>> for BadBatcher {
    type Error = MyError;
    fn load(&self, keys: &[i32]) -> BatchFuture<Result<i32, ValueError>, Self::Error> {
        ok(keys.into_iter()
                .map(|v| if v % 2 == 0 {
                    Ok(v * 10)
                } else {
                    Err(ValueError::NotEven)
                })
                .collect())
            .boxed()
    }
}