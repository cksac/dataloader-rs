use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::{Arc, Mutex},
};

use futures::{future, ready, task::Context, Future, FutureExt as _, Poll};

use super::{non_cached, LoadError};

#[derive(Clone)]
pub struct Loader<K, V, E, C>
where
    V: Clone,
    E: Clone,
    C: Cache<K, Result<V, LoadError<E>>>,
{
    loader: non_cached::Loader<K, V, E>,
    cache: Arc<Mutex<C>>,
}

impl<K, V, E, C> Loader<K, V, E, C>
where
    K: Clone,
    V: Clone,
    E: Clone,
    C: Cache<K, Result<V, LoadError<E>>>,
{
    pub fn load(&self, key: K) -> impl Future<Output = Result<V, LoadError<E>>>
    where
        V: Unpin,
    {
        let mut cache = self.cache.clone();
        if let Some(res) = self.cache.lock().unwrap().get(&key) {
            future::ready(res).left_future()
        } else {
            self.loader
                .load(key.clone())
                .map(move |res| {
                    cache.lock().unwrap().insert(key, res.clone());
                    res
                })
                .right_future()
        }
    }

    pub fn load_many(&self, keys: Vec<K>) -> impl Future<Output = Result<Vec<V>, LoadError<E>>>
    where
        V: Unpin,
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

#[derive(Clone)]
pub enum LoadFuture<V, E>
where
    V: Clone,
    E: Clone,
{
    Load(future::Shared<non_cached::LoadFuture<V, E>>),
    Prime(V),
}

impl<V, E> Future for LoadFuture<V, E>
where
    V: Clone + Unpin,
    E: Clone,
{
    type Output = Result<V, LoadError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        Poll::Ready(match *self {
            LoadFuture::Load(ref mut f) => ready!(f.poll_unpin(cx)).clone(),
            LoadFuture::Prime(ref v) => Ok(v.clone()),
        })
    }
}

impl<K, V, E, C> Loader<K, V, E, C>
where
    K: Clone + Ord,
    V: Clone,
    E: Clone,
    C: Cache<K, Result<V, LoadError<E>>>,
{
    pub fn with_cache(loader: non_cached::Loader<K, V, E>, cache: C) -> Self {
        Loader {
            loader,
            cache: Arc::new(Mutex::new(cache)),
        }
    }
}

impl<K, V, E> Loader<K, V, E, BTreeMap<K, Result<V, LoadError<E>>>>
where
    K: Clone + Ord,
    V: Clone,
    E: Clone,
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
