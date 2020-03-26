use crate::BatchFn;
use async_std::sync::{Arc, Mutex};
use async_std::task;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

struct State<K, V, E> {
    completed: HashMap<K, Result<V, E>>,
    pending: HashSet<K>,
}

impl<K, V, E> State<K, V, E> {
    fn new() -> Self {
        State {
            completed: HashMap::new(),
            pending: HashSet::new(),
        }
    }
}

pub struct Loader<K, V, E, F>
where
    K: Eq + Hash + Clone,
    V: Clone,
    E: Clone,
    F: BatchFn<K, V, Error = E>,
{
    state: Arc<Mutex<State<K, V, E>>>,
    load_fn: Arc<Mutex<F>>,
    max_batch_size: usize,
    yield_count: usize,
}

impl<K, V, E, F> Clone for Loader<K, V, E, F>
where
    K: Eq + Hash + Clone,
    V: Clone,
    E: Clone,
    F: BatchFn<K, V, Error = E>,
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

impl<K, V, E, F> Loader<K, V, E, F>
where
    K: Eq + Hash + Clone + Debug,
    V: Clone,
    E: Clone,
    F: BatchFn<K, V, Error = E>,
{
    pub fn new(load_fn: F) -> Loader<K, V, E, F> {
        Loader::with_yield_count(load_fn, 10)
    }

    pub fn with_yield_count(load_fn: F, yield_count: usize) -> Loader<K, V, E, F> {
        Loader {
            state: Arc::new(Mutex::new(State::new())),
            max_batch_size: load_fn.max_batch_size(),
            load_fn: Arc::new(Mutex::new(load_fn)),
            yield_count,
        }
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
            task::yield_now().await;
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
            task::yield_now().await;
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
}
