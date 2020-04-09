use crate::runtime::{yield_now, Arc, Mutex};
use crate::BatchFn;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash};

pub trait Cache {
    type Key;
    type Val;
    type Error;
    fn get(&self, key: &Self::Key) -> Option<&Result<Self::Val, Self::Error>>;
    fn insert(&mut self, key: Self::Key, val: Result<Self::Val, Self::Error>);
    fn remove(&mut self, key: &Self::Key) -> Option<Result<Self::Val, Self::Error>>;
    fn clear(&mut self);
}

impl<K, V, E, S: BuildHasher> Cache for HashMap<K, Result<V, E>, S>
where
    K: Eq + Hash,
{
    type Key = K;
    type Val = V;
    type Error = E;

    #[inline]
    fn get(&self, key: &K) -> Option<&Result<V, E>> {
        HashMap::get(self, key)
    }

    #[inline]
    fn insert(&mut self, key: K, val: Result<V, E>) {
        HashMap::insert(self, key, val);
    }

    #[inline]
    fn remove(&mut self, key: &K) -> Option<Result<V, E>> {
        HashMap::remove(self, key)
    }

    #[inline]
    fn clear(&mut self) {
        HashMap::clear(self)
    }
}

struct State<K, V, E, C = HashMap<K, Result<V, E>>>
where
    C: Cache<Key = K, Val = V, Error = E>,
{
    completed: C,
    pending: HashSet<K>,
}

impl<K: Eq + Hash, V, E, C> State<K, V, E, C>
where
    C: Cache<Key = K, Val = V, Error = E>,
{
    fn with_cache(cache: C) -> Self {
        State {
            completed: cache,
            pending: HashSet::new(),
        }
    }
}

pub struct Loader<K, V, E, F, C = HashMap<K, Result<V, E>>>
where
    K: Eq + Hash + Clone,
    V: Clone,
    E: Clone,
    F: BatchFn<K, V, Error = E>,
    C: Cache<Key = K, Val = V, Error = E>,
{
    state: Arc<Mutex<State<K, V, E, C>>>,
    load_fn: Arc<Mutex<F>>,
    yield_count: usize,
    pub max_batch_size: usize,
}

impl<K, V, E, F, C> Clone for Loader<K, V, E, F, C>
where
    K: Eq + Hash + Clone,
    V: Clone,
    E: Clone,
    F: BatchFn<K, V, Error = E>,
    C: Cache<Key = K, Val = V, Error = E>,
{
    fn clone(&self) -> Self {
        Loader {
            state: self.state.clone(),
            max_batch_size: self.max_batch_size,
            load_fn: self.load_fn.clone(),
            yield_count: self.yield_count,
        }
    }
}

#[allow(clippy::implicit_hasher)]
impl<K, V, E, F> Loader<K, V, E, F, HashMap<K, Result<V, E>>>
where
    K: Eq + Hash + Clone + Debug,
    V: Clone,
    E: Clone,
    F: BatchFn<K, V, Error = E>,
{
    pub fn new(load_fn: F) -> Loader<K, V, E, F, HashMap<K, Result<V, E>>> {
        Loader::with_cache(load_fn, HashMap::new())
    }
}

impl<K, V, E, F, C> Loader<K, V, E, F, C>
where
    K: Eq + Hash + Clone + Debug,
    V: Clone,
    E: Clone,
    F: BatchFn<K, V, Error = E>,
    C: Cache<Key = K, Val = V, Error = E>,
{
    pub fn with_cache(load_fn: F, cache: C) -> Loader<K, V, E, F, C> {
        Loader {
            state: Arc::new(Mutex::new(State::with_cache(cache))),
            load_fn: Arc::new(Mutex::new(load_fn)),
            max_batch_size: 200,
            yield_count: 10,
        }
    }

    pub fn with_max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.max_batch_size = max_batch_size;
        self
    }

    pub fn with_yield_count(mut self, yield_count: usize) -> Self {
        self.yield_count = yield_count;
        self
    }

    pub fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    pub async fn load(&self, key: K) -> Result<V, F::Error> {
        let mut state = self.state.lock().await;
        if let Some(v) = state.completed.get(&key) {
            return (*v).clone();
        }

        if state.pending.get(&key).is_none() {
            state.pending.insert(key.clone());
            if state.pending.len() >= self.max_batch_size {
                let keys = state.pending.drain().collect::<Vec<K>>();
                let load_fn = self.load_fn.lock().await;
                let load_ret = load_fn.load(keys.as_ref()).await;
                drop(load_fn);
                for (k, v) in load_ret.into_iter() {
                    state.completed.insert(k, v);
                }
                return state
                    .completed
                    .get(&key)
                    .cloned()
                    .unwrap_or_else(|| panic!("found key {:?} in load result", key));
            }
        }
        drop(state);

        // yield for other load to append request
        let mut i = 0;
        while i < self.yield_count {
            yield_now().await;
            i += 1;
        }

        let mut state = self.state.lock().await;
        if let Some(v) = state.completed.get(&key) {
            return (*v).clone();
        }

        if !state.pending.is_empty() {
            let keys = state.pending.drain().collect::<Vec<K>>();
            let load_fn = self.load_fn.lock().await;
            let load_ret = load_fn.load(keys.as_ref()).await;
            drop(load_fn);
            for (k, v) in load_ret.into_iter() {
                state.completed.insert(k, v);
            }
        }

        state
            .completed
            .get(&key)
            .cloned()
            .unwrap_or_else(|| panic!("found key {:?} in load result", key))
    }

    pub async fn load_many(&self, keys: Vec<K>) -> HashMap<K, Result<V, F::Error>> {
        let mut state = self.state.lock().await;
        let mut ret = HashMap::new();
        let mut rest = Vec::new();
        for key in keys.into_iter() {
            if let Some(v) = state.completed.get(&key).cloned() {
                ret.insert(key, v);
                continue;
            }
            if state.pending.get(&key).is_none() {
                state.pending.insert(key.clone());
                if state.pending.len() >= self.max_batch_size {
                    let keys = state.pending.drain().collect::<Vec<K>>();
                    let load_fn = self.load_fn.lock().await;
                    let load_ret = load_fn.load(keys.as_ref()).await;
                    drop(load_fn);
                    for (k, v) in load_ret.into_iter() {
                        state.completed.insert(k, v);
                    }
                }
            }
            rest.push(key);
        }
        drop(state);

        // yield for other load to append request
        let mut i = 0;
        while i < self.yield_count {
            yield_now().await;
            i += 1;
        }

        if !rest.is_empty() {
            let mut state = self.state.lock().await;
            if !state.pending.is_empty() {
                let keys = state.pending.drain().collect::<Vec<K>>();
                let load_fn = self.load_fn.lock().await;
                let load_ret = load_fn.load(keys.as_ref()).await;
                drop(load_fn);
                for (k, v) in load_ret.into_iter() {
                    state.completed.insert(k, v);
                }
            }

            for key in rest.into_iter() {
                let v = state
                    .completed
                    .get(&key)
                    .cloned()
                    .unwrap_or_else(|| panic!("found key {:?} in load result", key));
                ret.insert(key, v);
            }
        }

        ret
    }

    pub async fn prime(&self, key: K, val: V) {
        let mut state = self.state.lock().await;
        state.completed.insert(key, Ok(val));
    }

    pub async fn clear(&self, key: K) {
        let mut state = self.state.lock().await;
        state.completed.remove(&key);
    }

    pub async fn clear_all(&self) {
        let mut state = self.state.lock().await;
        state.completed.clear()
    }
}
