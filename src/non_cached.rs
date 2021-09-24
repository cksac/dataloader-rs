use crate::runtime::{yield_now, Arc, Mutex};
use crate::BatchFn;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::io::{ErrorKind, Error};

type RequestId = usize;

struct State<K, V> {
    completed: HashMap<RequestId, V>,
    pending: HashMap<RequestId, K>,
    id_seq: RequestId,
}

impl<K, V> State<K, V> {
    fn new() -> Self {
        State {
            completed: HashMap::new(),
            pending: HashMap::new(),
            id_seq: 0,
        }
    }
    fn next_request_id(&mut self) -> RequestId {
        self.id_seq = self.id_seq.wrapping_add(1);
        self.id_seq
    }
}

pub struct Loader<K, V, F>
    where
        K: Eq + Hash + Clone,
        V: Clone,
        F: BatchFn<K, V>,
{
    state: Arc<Mutex<State<K, V>>>,
    load_fn: Arc<Mutex<F>>,
    yield_count: usize,
    max_batch_size: usize,
}

impl<K, V, F> Clone for Loader<K, V, F>
    where
        K: Eq + Hash + Clone,
        V: Clone,
        F: BatchFn<K, V>,
{
    fn clone(&self) -> Self {
        Loader {
            state: self.state.clone(),
            load_fn: self.load_fn.clone(),
            max_batch_size: self.max_batch_size,
            yield_count: self.yield_count,
        }
    }
}

impl<K, V, F> Loader<K, V, F>
    where
        K: Eq + Hash + Clone + Debug,
        V: Clone,
        F: BatchFn<K, V>,
{
    pub fn new(load_fn: F) -> Loader<K, V, F> {
        Loader {
            state: Arc::new(Mutex::new(State::new())),
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

    pub async fn load_safe(&self, key: K) -> Result<V, Error> {
        let mut state = self.state.lock().await;
        let request_id = state.next_request_id();
        state.pending.insert(request_id, key);
        if state.pending.len() >= self.max_batch_size {
            let batch = state.pending.drain().collect::<HashMap<usize, K>>();
            let keys: Vec<K> = batch
                .values()
                .cloned()
                .collect::<HashSet<K>>()
                .into_iter()
                .collect();
            let mut load_fn = self.load_fn.lock().await;
            let load_ret = load_fn.load(keys.as_ref()).await;
            drop(load_fn);
            for (request_id, key) in batch.into_iter() {
                state.completed.insert(
                    request_id,
                    load_ret
                        .get(&key)
                        .ok_or(Error::new(ErrorKind::NotFound, format!("could not lookup result for given key: {:?}", key)))?
                        .clone(),
                );
            }
            return Ok(state.completed.remove(&request_id).expect("completed"));
        }
        drop(state);

        // yield for other load to append request
        let mut i = 0;
        while i < self.yield_count {
            yield_now().await;
            i += 1;
        }

        let mut state = self.state.lock().await;

        if state.completed.get(&request_id).is_none() {
            let batch = state.pending.drain().collect::<HashMap<usize, K>>();
            if !batch.is_empty() {
                let keys: Vec<K> = batch
                    .values()
                    .cloned()
                    .collect::<HashSet<K>>()
                    .into_iter()
                    .collect();
                let mut load_fn = self.load_fn.lock().await;
                let load_ret = load_fn.load(keys.as_ref()).await;
                drop(load_fn);
                for (request_id, key) in batch.into_iter() {
                    state.completed.insert(
                        request_id,
                        load_ret
                            .get(&key)
                            .ok_or(Error::new(ErrorKind::NotFound, format!("could not lookup result for given key: {:?}", key)))?
                            .clone(),
                    );
                }
            }
        }
        Ok(state.completed.remove(&request_id).expect("completed"))
    }

    pub async fn load(&self, key: K) -> V {
        self.load_safe(key).await.unwrap_or_else(|e| panic!("{}", e))
    }

    pub async fn load_many(&self, keys: Vec<K>) -> HashMap<K, V> {
        self.load_many_safe(keys).await.unwrap_or_else(|e| panic!("{}", e))
    }

    pub async fn load_many_safe(&self, keys: Vec<K>) -> Result<HashMap<K, V>, Error> {
        let mut state = self.state.lock().await;
        let mut ret = HashMap::new();
        let mut requests = Vec::new();
        for key in keys.into_iter() {
            let request_id = state.next_request_id();
            requests.push((request_id, key.clone()));
            state.pending.insert(request_id, key);
            if state.pending.len() >= self.max_batch_size {
                let batch = state.pending.drain().collect::<HashMap<usize, K>>();
                let keys: Vec<K> = batch
                    .values()
                    .cloned()
                    .collect::<HashSet<K>>()
                    .into_iter()
                    .collect();
                let mut load_fn = self.load_fn.lock().await;
                let load_ret = load_fn.load(keys.as_ref()).await;
                drop(load_fn);
                for (request_id, key) in batch.into_iter() {
                    state.completed.insert(
                        request_id,
                        load_ret
                            .get(&key)
                            .ok_or(Error::new(ErrorKind::NotFound, format!("could not lookup result for given key: {:?}", key)))?
                            .clone(),
                    );
                }
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

        let mut rest = Vec::new();
        for (request_id, key) in requests.into_iter() {
            if let Some(v) = state.completed.remove(&request_id) {
                ret.insert(key, v);
            } else {
                rest.push((request_id, key));
            }
        }

        if !rest.is_empty() {
            let batch = state.pending.drain().collect::<HashMap<usize, K>>();
            if !batch.is_empty() {
                let keys: Vec<K> = batch
                    .values()
                    .cloned()
                    .collect::<HashSet<K>>()
                    .into_iter()
                    .collect();
                let mut load_fn = self.load_fn.lock().await;
                let load_ret = load_fn.load(keys.as_ref()).await;
                drop(load_fn);
                for (request_id, key) in batch.into_iter() {
                    state.completed.insert(
                        request_id,
                        load_ret
                            .get(&key)
                            .ok_or(Error::new(ErrorKind::NotFound, format!("could not lookup result for given key: {:?}", key)))?
                            .clone(),
                    );
                }
            }
            for (request_id, key) in rest.into_iter() {
                let v = state
                    .completed
                    .remove(&request_id)
                    .ok_or(Error::new(ErrorKind::NotFound, format!("could not lookup result for given key: {:?}", key)))?;

                ret.insert(key, v);
            }
        }

        Ok(ret)
    }
}
