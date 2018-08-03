use LoadError;
use non_cached;

use std::sync::{Arc, Mutex};
use std::collections::BTreeMap;

use futures::{Future, Poll, Async};
use futures::future::{join_all, JoinAll, Shared};


#[derive(Clone)]
pub struct Loader<K, V, E, C>
    where V: Clone,
          E: Clone,
          C: Cache<K, LoadFuture<V, E>>
{
    loader: non_cached::Loader<K, V, E>,
    cache: Arc<Mutex<C>>,
}

impl<K, V, E, C> Loader<K, V, E, C>
    where K: Clone + Ord,
          V: Clone,
          E: Clone,
          C: Cache<K, LoadFuture<V, E>>
{
    pub fn load(&self, key: K) -> LoadFuture<V, E> {
        let mut cache = self.cache.lock().unwrap();
        if let Some(v) = cache.get(&key) {
            v
        } else {
            let shared = self.loader.load(key.clone()).shared();
            let f = LoadFuture::Load(shared);
            cache.insert(key, f.clone());
            f
        }
    }

    pub fn load_many(&self, keys: Vec<K>) -> JoinAll<Vec<LoadFuture<V, E>>> {
        join_all(keys.into_iter().map(|v| self.load(v)).collect())
    }

    pub fn remove(&self, key: &K) -> Option<LoadFuture<V, E>> {
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
            cache.insert(key, LoadFuture::Prime(val));
        }
    }
}

#[derive(Clone)]
pub enum LoadFuture<V, E>
    where V: Clone,
          E: Clone
{
    Load(Shared<non_cached::LoadFuture<V, E>>),
    Prime(V),
}

impl<V, E> Future for LoadFuture<V, E>
    where V: Clone,
          E: Clone
{
    type Item = V;
    type Error = LoadError<E>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            LoadFuture::Load(ref mut f) => {
                match f.poll() {
                    Ok(Async::NotReady) => Ok(Async::NotReady),
                    Ok(Async::Ready(shared)) => Ok(Async::Ready((*shared).clone())),
                    Err(e) => Err((*e).clone()),
                }
            }
            LoadFuture::Prime(ref v) => Ok(Async::Ready(v.clone())),
        }
    }
}

impl<K, V, E, C> Loader<K, V, E, C>
    where K: Clone + Ord,
          V: Clone,
          E: Clone,
          C: Cache<K, LoadFuture<V, E>>
{
    pub fn with_cache(loader: non_cached::Loader<K, V, E>, cache: C) -> Self {
        Loader {
            loader,
            cache: Arc::new(Mutex::new(cache)),
        }
    }
}

impl<K, V, E> Loader<K, V, E, BTreeMap<K, LoadFuture<V, E>>>
    where K: Clone + Ord,
          V: Clone,
          E: Clone
{
    pub fn new(loader: non_cached::Loader<K, V, E>) -> Self {
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
    where K: Ord,
          V: Clone
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
