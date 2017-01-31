use loader as non_cached;
use loader::LoadError;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use futures::{Future, Poll, Async};
use futures::future::{join_all, JoinAll, Shared};

#[derive(Clone)]
pub struct Loader<K, V, E>
    where V: Clone,
          E: Clone
{
    loader: non_cached::Loader<K, V, E>,
    cache: RefCell<BTreeMap<K, Shared<non_cached::LoadFuture<V, E>>>>,
}

impl<K, V, E> Loader<K, V, E>
    where K: Clone + Ord,
          V: Clone,
          E: Clone
{
    pub fn load(&self, key: K) -> LoadFuture<V, E> {
        match self.cache.borrow_mut().entry(key.clone()) {
            Entry::Vacant(v) => {
                let f = self.loader.load(key).shared();
                v.insert(f.clone());
                LoadFuture { f: f }
            }
            Entry::Occupied(e) => {
                let f = e.get().clone();
                LoadFuture { f: f }
            }
        }
    }

    pub fn load_many(&self, keys: Vec<K>) -> JoinAll<Vec<LoadFuture<V, E>>> {
        join_all(keys.into_iter().map(|v| self.load(v)).collect())
    }
}

pub struct LoadFuture<V, E>
    where V: Clone,
          E: Clone
{
    f: Shared<non_cached::LoadFuture<V, E>>,
}

impl<V, E> Future for LoadFuture<V, E>
    where V: Clone,
          E: Clone
{
    type Item = V;
    type Error = LoadError<E>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.f.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(shared)) => Ok(Async::Ready(shared.clone())),
            Err(e) => Err(e.clone()),
        }
    }
}

impl<K, V, E> Loader<K, V, E>
    where K: Clone + Ord,
          V: Clone,
          E: Clone
{
    pub fn new(loader: non_cached::Loader<K, V, E>) -> Loader<K, V, E> {
        Loader {
            loader: loader,
            cache: RefCell::new(BTreeMap::new()),
        }
    }
}
