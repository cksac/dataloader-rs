use crate::runtime::{Arc, Mutex};
use crate::{yield_fn, BatchFn, WaitForWorkFn};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::io::{Error, ErrorKind};

type RequestId = usize;

struct State<K, V> {
    completed: HashMap<RequestId, V>,
    failed: HashMap<RequestId, K>,
    pending: HashMap<RequestId, K>,
    id_seq: RequestId,
}

impl<K, V> State<K, V> {
    fn new() -> Self {
        State {
            completed: HashMap::new(),
            failed: HashMap::new(),
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
    wait_for_work_fn: Arc<dyn WaitForWorkFn>,
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
            wait_for_work_fn: self.wait_for_work_fn.clone(),
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
            wait_for_work_fn: Arc::new(yield_fn(10)),
        }
    }

    pub fn with_max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.max_batch_size = max_batch_size;
        self
    }

    pub fn with_yield_count(mut self, yield_count: usize) -> Self {
        self.wait_for_work_fn = Arc::new(yield_fn(yield_count));
        self
    }

    /// Replaces the yielding for work behavior with an arbitrary future. Rather than yielding
    /// the runtime repeatedly this will generate and `.await` a future of your choice.
    /// ***This is incompatible with*** [`Self::with_yield_count()`].
    pub fn with_custom_wait_for_work(mut self, wait_for_work_fn: impl WaitForWorkFn) {
        self.wait_for_work_fn = Arc::new(wait_for_work_fn);
    }

    pub fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    pub async fn try_load(&self, key: K) -> Result<V, Error> {
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
                if load_ret
                    .get(&key)
                    .and_then(|v| state.completed.insert(request_id, v.clone()))
                    .is_none()
                {
                    state.failed.insert(request_id, key);
                }
            }
            return state.completed.remove(&request_id).ok_or_else(|| {
                Error::new(
                    ErrorKind::NotFound,
                    format!(
                        "could not lookup result for given key: {:?}",
                        state.failed.remove(&request_id).expect("failed")
                    ),
                )
            });
        }
        drop(state);

        (self.wait_for_work_fn)().await;

        let mut state = self.state.lock().await;

        if !state.completed.contains_key(&request_id) {
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
                    if load_ret
                        .get(&key)
                        .and_then(|v| state.completed.insert(request_id, v.clone()))
                        .is_none()
                    {
                        state.failed.insert(request_id, key);
                    }
                }
            }
        }
        state.completed.remove(&request_id).ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!(
                    "could not lookup result for given key: {:?}",
                    state.failed.remove(&request_id).expect("failed")
                ),
            )
        })
    }

    pub async fn load(&self, key: K) -> V {
        self.try_load(key).await.unwrap_or_else(|e| panic!("{}", e))
    }

    pub async fn load_many(&self, keys: Vec<K>) -> HashMap<K, V> {
        self.try_load_many(keys)
            .await
            .unwrap_or_else(|e| panic!("{}", e))
    }

    pub async fn try_load_many(&self, keys: Vec<K>) -> Result<HashMap<K, V>, Error> {
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
                    if load_ret
                        .get(&key)
                        .and_then(|v| state.completed.insert(request_id, v.clone()))
                        .is_none()
                    {
                        state.failed.insert(request_id, key);
                    }
                }
            }
        }

        drop(state);

        (self.wait_for_work_fn)().await;

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
                    if load_ret
                        .get(&key)
                        .and_then(|v| state.completed.insert(request_id, v.clone()))
                        .is_none()
                    {
                        state.failed.insert(request_id, key);
                    }
                }
            }
            for (request_id, key) in rest.into_iter() {
                let v = state.completed.remove(&request_id).ok_or_else(|| {
                    Error::new(
                        ErrorKind::NotFound,
                        format!(
                            "could not lookup result for given key: {:?}",
                            state.failed.remove(&request_id).expect("failed")
                        ),
                    )
                })?;

                ret.insert(key, v);
            }
        }

        Ok(ret)
    }
}
