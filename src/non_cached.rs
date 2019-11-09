use futures::{future, ready, task::Context, FutureExt as _};
use futures_timer::Delay;
use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    mem,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

use super::{cached, BatchFn, BatchFuture, LoadError};

pub struct Loader<K, V, E, F> {
    state: Arc<Mutex<State<K, Result<V, LoadError<E>>, F, BatchFuture<V, E>>>>,
}

// Manual implementation is used to omit applying unnecessary Clone bounds.
impl<K, V, E, F> Clone for Loader<K, V, E, F> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<K, V, E, F> Loader<K, V, E, F> {
    pub fn new(batch_fn: F) -> Loader<K, V, E, F>
    where
        E: Clone,
        F: BatchFn<K, V, Error = E> + 'static,
    {
        let max_batch_size = batch_fn.max_batch_size();
        assert!(max_batch_size > 0);

        Loader {
            state: Arc::new(Mutex::new(State {
                autoinc: 0,
                max_batch_size,
                load_fn: batch_fn,
                queued_keys: HashMap::new(),
                loading_ids: HashMap::new(),
                loading_batches: HashMap::new(),
                loaded_vals: HashMap::new(),
            })),
        }
    }

    pub fn load(&self, key: K) -> LoadFuture<K, V, E, F>
    where
        E: Clone,
    {
        let id = {
            let mut st = self.state.lock().unwrap();
            let id = st.new_unique_id();
            st.queued_keys.insert(id, key);
            id
        };
        LoadFuture {
            id,
            delay: None,
            stage: Stage::Created,
            state: self.state.clone(),
        }
    }

    pub fn load_many(&self, keys: Vec<K>) -> future::TryJoinAll<LoadFuture<K, V, E, F>>
    where
        E: Clone,
        F: BatchFn<K, V, Error = E>,
    {
        future::try_join_all(keys.into_iter().map(|v| self.load(v)))
    }

    pub fn cached(self) -> cached::Loader<K, V, E, F, BTreeMap<K, cached::Item<K, V, E, F>>>
    where
        K: Ord,
        E: Clone,
        F: BatchFn<K, V, Error = E>,
    {
        cached::Loader::new(self)
    }

    pub fn with_cache<C>(self, cache: C) -> cached::Loader<K, V, E, F, C> {
        cached::Loader::with_cache(self, cache)
    }
}

enum Stage {
    Created,
    Polled,
    Finished,
}

pub struct LoadFuture<K, V, E, F> {
    id: usize,
    delay: Option<Delay>,
    stage: Stage,
    state: Arc<Mutex<State<K, Result<V, LoadError<E>>, F, BatchFuture<V, E>>>>,
}

impl<K, V, E, F> Future for LoadFuture<K, V, E, F>
where
    E: Clone,
    F: BatchFn<K, V, Error = E>,
{
    type Output = Result<V, LoadError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let state = self.state.clone();
        let mut st = state.lock().unwrap();

        if st.loaded_vals.contains_key(&self.id) {
            self.stage = Stage::Finished;
            return Poll::Ready(st.loaded_vals.remove(&self.id).unwrap());
        }

        if let Some(batch_id) = st.loading_ids.get(&self.id) {
            let batch_id = *batch_id;
            ready!(st.poll_batch(cx, batch_id));
            self.stage = Stage::Finished;
            return Poll::Ready(st.loaded_vals.remove(&self.id).unwrap());
        }

        if self.delay.is_some() {
            let _ = ready!(self.delay.as_mut().unwrap().poll_unpin(cx));
            self.delay = None;
        }

        if let Stage::Polled = self.stage {
            let batch_id = st.dispatch_new_batch(self.id);
            ready!(st.poll_batch(cx, batch_id));
            self.stage = Stage::Finished;
            return Poll::Ready(st.loaded_vals.remove(&self.id).unwrap());
        }

        // Skipping first poll for LoadFuture allows to defer a batch loading,
        // and collect more keys for the batch.
        if let Stage::Created = self.stage {
            self.stage = Stage::Polled;
        }
        // Reschedule this LoadFuture execution to the end of event loop.
        // This allows to defer the actual loading much better, as leaves space
        // for more event loop ticks to happen before doing actual loading,
        // where new keys may be enqueued.
        self.delay = Some(Delay::new(Duration::from_nanos(1)));
        let _ = ready!(self.delay.as_mut().unwrap().poll_unpin(cx));
        // If Delay Future is somehow ready instantly (normally, this should not
        // happen), then defer this LoadFuture execution with Waker, which is
        // not as good as Delay Future in deferring, but is something at least.
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl<K, V, E, F> Drop for LoadFuture<K, V, E, F> {
    fn drop(&mut self) {
        if let Stage::Finished = self.stage {
            return;
        }
        let state = self.state.clone();
        let mut st = match state.lock() {
            Ok(st) => st,
            Err(_) => return, // Do not panic if Mutex is poisoned.
        };

        st.queued_keys.remove(&self.id);
        st.loaded_vals.remove(&self.id);
        if let Some(batch_id) = st.loading_ids.remove(&self.id) {
            let mut drop_loading_batch = true;
            for id in &st.loading_batches[&batch_id].ids {
                if st.loading_ids.contains_key(id) {
                    drop_loading_batch = false;
                    break;
                }
            }
            // If everything in a loading batch has been dropped,
            // then we can drop the whole batch eagerly.
            if drop_loading_batch {
                st.loading_batches.remove(&batch_id);
            }
        }
    }
}

struct State<K, V, F, Fut> {
    autoinc: usize,
    max_batch_size: usize,
    load_fn: F,
    queued_keys: HashMap<usize, K>,
    loading_ids: HashMap<usize, usize>,
    loading_batches: HashMap<usize, LoadingBatch<Fut>>,
    loaded_vals: HashMap<usize, V>,
}

impl<K, V, E, F> State<K, Result<V, LoadError<E>>, F, BatchFuture<V, E>>
where
    E: Clone,
    F: BatchFn<K, V, Error = E>,
{
    fn poll_batch(&mut self, cx: &mut Context, batch_id: usize) -> Poll<()> {
        let (result, ids) = {
            let batch = self.loading_batches.get_mut(&batch_id).unwrap();
            let result = ready!(Pin::new(&mut batch.fut).poll(cx));
            let ids = mem::replace(&mut batch.ids, vec![]);
            (result, ids)
        };
        match result {
            Ok(vals) => {
                if vals.len() != ids.len() {
                    let err = LoadError::UnequalKeyValueSize {
                        key_count: ids.len(),
                        value_count: vals.len(),
                    };
                    for id in ids {
                        if let Some(_) = self.loading_ids.remove(&id) {
                            self.loaded_vals.insert(id, Err(err.clone()));
                        }
                    }
                } else {
                    for (id, v) in ids.into_iter().zip(vals.into_iter()) {
                        if let Some(_) = self.loading_ids.remove(&id) {
                            self.loaded_vals.insert(id, Ok(v));
                        }
                    }
                }
            }
            Err(err) => {
                let err = LoadError::BatchFn(err);
                for id in ids {
                    if let Some(_) = self.loading_ids.remove(&id) {
                        self.loaded_vals.insert(id, Err(err.clone()));
                    }
                }
            }
        }
        self.loading_batches.remove(&batch_id);
        Poll::Ready(())
    }

    fn dispatch_new_batch(&mut self, with_id: usize) -> usize {
        let size = self.max_batch_size.min(self.queued_keys.len());

        let batch_id = self.new_unique_id();

        let mut ids = Vec::with_capacity(size);
        let mut keys = Vec::with_capacity(size);
        ids.push(with_id);
        keys.push(self.queued_keys.remove(&with_id).unwrap());
        self.loading_ids.insert(with_id, batch_id);
        if size > 1 {
            for id in self.queued_keys.keys().take(size - 1) {
                ids.push(*id);
            }
        }
        for id in ids.iter().skip(1) {
            keys.push(self.queued_keys.remove(id).unwrap());
            self.loading_ids.insert(*id, batch_id);
        }

        self.loading_batches.insert(
            batch_id,
            LoadingBatch {
                ids,
                fut: self.load_fn.load(&keys),
            },
        );

        batch_id
    }
}

impl<K, V, F, Fut> State<K, V, F, Fut> {
    fn new_unique_id(&mut self) -> usize {
        self.autoinc = self.autoinc.checked_add(1).unwrap_or(0);
        self.autoinc
    }
}

struct LoadingBatch<Fut> {
    ids: Vec<usize>,
    fut: Fut,
}
