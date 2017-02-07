use {BatchFn, BatchFuture};
use cached;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;
use std::hash::Hash;

use futures::Future;
use futures::future::{err, ok};

mod non_cached_loader;
mod cached_loader;

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

impl BatchFn<i32, ()> for BadBatcher {
    type Error = ();
    fn load(&self, _keys: &[i32]) -> BatchFuture<(), Self::Error> {
        //always return less values compared to request keys
        ok(vec![]).boxed()
    }
}

pub struct MyCache<K, V>(HashMap<K, V>);
impl<K, V> MyCache<K, V>
    where K: Ord + Hash,
          V: Clone
{
    pub fn new() -> MyCache<K, V> {
        MyCache(HashMap::new())
    }
}

impl<K, V> cached::Cache<K, V> for MyCache<K, V>
    where K: Ord + Hash,
          V: Clone
{
    fn contains_key(&self, key: &K) -> bool {
        HashMap::contains_key(&self.0, key)
    }

    fn get(&self, key: &K) -> Option<V> {
        HashMap::get(&self.0, key).map(|v| v.clone())
    }

    fn insert(&mut self, key: K, value: V) {
        HashMap::insert(&mut self.0, key, value);
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        HashMap::remove(&mut self.0, key)
    }

    fn clear(&mut self) {
        HashMap::clear(&mut self.0);
    }
}