use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use futures::{future, Future, FutureExt as _};

use super::{non_cached, BatchFn, LoadError};

pub struct Loader<K, V, E, F, C>
where
    V: Clone,
    E: Clone,
    C: Cache<K, Result<V, LoadError<E>>>,
{
    loader: non_cached::Loader<K, V, E, F>,
    cache: Arc<Mutex<C>>,
}

impl<K, V, E, F, C> Clone for Loader<K, V, E, F, C>
where
    V: Clone,
    E: Clone,
    C: Cache<K, Result<V, LoadError<E>>>,
{
    fn clone(&self) -> Self {
        Self {
            loader: self.loader.clone(),
            cache: self.cache.clone(),
        }
    }
}

impl<K, V, E, F, C> Loader<K, V, E, F, C>
where
    K: Clone,
    V: Clone,
    E: Clone,
    C: Cache<K, Result<V, LoadError<E>>>,
{
    pub fn load(&self, key: K) -> impl Future<Output = Result<V, LoadError<E>>>
    where
        V: Unpin,
        F: BatchFn<K, V, Error = E>,
    {
        if let Some(res) = self.cache.lock().unwrap().get(&key) {
            return future::ready(res).left_future();
        }
        let cache = self.cache.clone();
        let loader = self.loader.clone();
        future::lazy(move |_| {
            if let Some(res) = cache.lock().unwrap().get(&key) {
                return future::ready(res).left_future();
            }
            loader
                .load(key.clone())
                .map(move |res| {
                    cache.lock().unwrap().insert(key, res.clone());
                    res
                })
                .right_future()
        })
        .flatten()
        .right_future()
    }

    pub fn load_many(&self, keys: Vec<K>) -> impl Future<Output = Result<Vec<V>, LoadError<E>>>
    where
        V: Unpin,
        F: BatchFn<K, V, Error = E>,
    {
        future::try_join_all(keys.into_iter().map(|v| self.load(v)))
    }

    pub fn remove(&self, key: &K) -> Option<Result<V, LoadError<E>>> {
        let mut cache = self.cache.lock().unwrap();
        cache.remove(key)
    }

    pub fn clear(&self) {
        let mut cache = self.cache.lock().unwrap();
        cache.clear();
    }

    pub fn prime(&self, key: K, val: V) {
        let mut cache = self.cache.lock().unwrap();
        if !cache.contains_key(&key) {
            cache.insert(key, Ok(val));
        }
    }
}

impl<K, V, E, F, C> Loader<K, V, E, F, C>
where
    K: Clone + Ord,
    V: Clone,
    E: Clone,
    C: Cache<K, Result<V, LoadError<E>>>,
{
    pub fn with_cache(loader: non_cached::Loader<K, V, E, F>, cache: C) -> Self {
        Loader {
            loader,
            cache: Arc::new(Mutex::new(cache)),
        }
    }
}

impl<K, V, E, F> Loader<K, V, E, F, BTreeMap<K, Result<V, LoadError<E>>>>
where
    K: Clone + Ord,
    V: Clone,
    E: Clone,
{
    pub fn new(loader: non_cached::Loader<K, V, E, F>) -> Self {
        Loader::with_cache(loader, BTreeMap::new())
    }
}

pub trait Cache<K, V> {
    fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }
    fn get(&self, key: &K) -> Option<V>;
    fn insert(&mut self, key: K, value: V);
    fn remove(&mut self, key: &K) -> Option<V>;
    fn clear(&mut self);
}

impl<K, V> Cache<K, V> for BTreeMap<K, V>
where
    K: Ord,
    V: Clone,
{
    fn contains_key(&self, key: &K) -> bool {
        BTreeMap::contains_key(self, key)
    }

    fn get(&self, key: &K) -> Option<V> {
        BTreeMap::get(self, key).map(|v| (*v).clone())
    }

    fn insert(&mut self, key: K, value: V) {
        BTreeMap::insert(self, key, value);
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        BTreeMap::remove(self, key)
    }

    fn clear(&mut self) {
        BTreeMap::clear(self);
    }
}
