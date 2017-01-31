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
    cache: RefCell<BTreeMap<K, LoadFuture<V, E>>>,
}

impl<K, V, E> Loader<K, V, E>
    where K: Clone + Ord,
          V: Clone,
          E: Clone
{
    pub fn load(&self, key: K) -> LoadFuture<V, E> {
        match self.cache.borrow_mut().entry(key.clone()) {
            Entry::Vacant(v) => {
                let shared = self.loader.load(key).shared();
                let f = LoadFuture::Load(shared);
                v.insert(f.clone());
                f
            }
            Entry::Occupied(e) => e.get().clone(),
        }
    }

    pub fn load_many(&self, keys: Vec<K>) -> JoinAll<Vec<LoadFuture<V, E>>> {
        join_all(keys.into_iter().map(|v| self.load(v)).collect())
    }

    pub fn clear(&self, key: &K) -> Option<LoadFuture<V, E>> {
        self.cache.borrow_mut().remove(key)
    }

    pub fn clear_all(&self) {
        self.cache.borrow_mut().clear();
    }

    pub fn prime(&self, key: K, val: V) {
        if let Entry::Vacant(v) = self.cache.borrow_mut().entry(key) {
            v.insert(LoadFuture::Prime(val));
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
                    Ok(Async::Ready(shared)) => Ok(Async::Ready(shared.clone())),
                    Err(e) => Err(e.clone()),
                }
            }
            LoadFuture::Prime(ref v) => Ok(Async::Ready(v.clone())),
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
