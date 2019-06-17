use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use futures::{future, Future, FutureExt as _};

use super::{
    non_cached::{self, LoadFuture},
    BatchFn, LoadError,
};

pub struct Loader<K, V, E, F, C> {
    loader: non_cached::Loader<K, V, E, F>,
    cache: Arc<Mutex<C>>,
}

// Manual implementation is used to omit applying unnecessary Clone bounds.
impl<K, V, E, F, C> Clone for Loader<K, V, E, F, C> {
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
    F: BatchFn<K, V, Error = E>,
    C: Cache<K, Item<K, V, E, F>>,
{
    pub fn load(&self, key: K) -> impl Future<Output = Result<V, LoadError<E>>>
    where
        V: Unpin,
        F: BatchFn<K, V, Error = E>,
    {
        let mut cache = self.cache.lock().unwrap();
        if let Some(item) = cache.get(&key) {
            return item.into_future().left_future();
        }
        let item = CacheItem::Loading(self.loader.load(key.clone()).shared());
        cache.insert(key, item.clone());
        item.into_future().right_future()
    }

    pub fn load_many(&self, keys: Vec<K>) -> impl Future<Output = Result<Vec<V>, LoadError<E>>>
    where
        V: Unpin,
        F: BatchFn<K, V, Error = E>,
    {
        future::try_join_all(keys.into_iter().map(|v| self.load(v)))
    }

    pub fn remove(&self, key: &K) -> Option<impl Future<Output = Result<V, LoadError<E>>>> {
        let mut cache = self.cache.lock().unwrap();
        cache.remove(key).map(|item| item.into_future())
    }

    pub fn clear(&self) {
        let mut cache = self.cache.lock().unwrap();
        cache.clear();
    }

    pub fn prime(&self, key: K, val: V) {
        let mut cache = self.cache.lock().unwrap();
        if !cache.contains_key(&key) {
            cache.insert(key, CacheItem::Prime(Ok(val)));
        }
    }
}

impl<K, V, E, F, C> Loader<K, V, E, F, C> {
    pub fn with_cache(loader: non_cached::Loader<K, V, E, F>, cache: C) -> Self {
        Loader {
            loader,
            cache: Arc::new(Mutex::new(cache)),
        }
    }
}

impl<K, V, E, F> Loader<K, V, E, F, BTreeMap<K, Item<K, V, E, F>>>
where
    K: Ord,
    E: Clone,
    F: BatchFn<K, V, Error = E>,
{
    pub fn new(loader: non_cached::Loader<K, V, E, F>) -> Self {
        Loader::with_cache(loader, BTreeMap::new())
    }
}

pub type Item<K, V, E, F> = CacheItem<Result<V, LoadError<E>>, LoadFuture<K, V, E, F>>;

pub enum CacheItem<V, F: Future> {
    Prime(V),
    Loading(future::Shared<F>),
}

// Manual implementation is used to omit applying unnecessary Clone bounds.
impl<V, F> Clone for CacheItem<V, F>
where
    V: Clone,
    F: Future,
{
    fn clone(&self) -> Self {
        match self {
            CacheItem::Prime(ref v) => CacheItem::Prime(v.clone()),
            CacheItem::Loading(ref f) => CacheItem::Loading(f.clone()),
        }
    }
}

impl<V, F> CacheItem<V, F>
where
    V: Clone,
    F: Future<Output = V>,
{
    pub fn into_future(self) -> impl Future<Output = V> {
        match self {
            CacheItem::Prime(v) => future::ready(v).left_future(),
            CacheItem::Loading(f) => f.right_future(),
        }
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
