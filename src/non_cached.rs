use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    mem,
    pin::Pin,
    sync::{Arc, Mutex},
};

use futures::{
    future, ready,
    task::{Context, Waker},
    Future, Poll,
};

use super::{cached, BatchFn, BatchFuture, LoadError};

pub struct Loader<K, V, E, F> {
    state: Arc<Mutex<State<K, Result<V, LoadError<E>>, BatchFuture<V, E>>>>,
    batch_fn: Arc<F>,
    max_batch_size: usize,
}

impl<K, V, E, F> Clone for Loader<K, V, E, F> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            batch_fn: self.batch_fn.clone(),
            max_batch_size: self.max_batch_size,
        }
    }
}

impl<K, V, E, F> Loader<K, V, E, F> {
    pub fn new(batch_fn: F) -> Loader<K, V, E, F>
    where
        E: Clone,
        F: BatchFn<K, V, Error = E> + 'static,
    {
        assert!(batch_fn.max_batch_size() > 0);

        let max_batch_size = batch_fn.max_batch_size();
        Loader {
            state: Arc::new(Mutex::new(State {
                polls: 0,
                last_index: 0,
                queue: VecDeque::new(),
                queued_keys: HashMap::new(),
                queued_wakers: HashMap::new(),
                loading: None,
                loaded: HashMap::new(),
            })),
            batch_fn: Arc::new(batch_fn),
            max_batch_size,
        }
    }

    pub fn load(&self, key: K) -> LoadFuture<K, V, E, F>
    where
        E: Clone,
    {
        let id: usize;
        {
            let mut st = self.state.lock().unwrap();
            id = st.last_index.checked_add(1).unwrap_or(0);
            st.queue.push_back(id);
            st.queued_keys.insert(id, key);
            st.last_index = id;
        }
        LoadFuture {
            id,
            stage: Stage::Enqueued,
            loader: self.clone(),
        }
    }

    pub fn load_many(&self, keys: Vec<K>) -> future::TryJoinAll<LoadFuture<K, V, E, F>>
    where
        E: Clone,
        F: BatchFn<K, V, Error = E>,
    {
        future::try_join_all(keys.into_iter().map(|v| self.load(v)))
    }

    pub fn cached(self) -> cached::Loader<K, V, E, F, BTreeMap<K, Result<V, LoadError<E>>>>
    where
        K: Clone + Ord,
        V: Clone,
        E: Clone,
    {
        cached::Loader::new(self)
    }

    pub fn with_cache<C>(self, cache: C) -> cached::Loader<K, V, E, F, C>
    where
        K: Clone + Ord,
        V: Clone,
        E: Clone,
        C: cached::Cache<K, Result<V, LoadError<E>>>,
    {
        cached::Loader::with_cache(self, cache)
    }
}

pub struct LoadFuture<K, V, E, F> {
    id: usize,
    stage: Stage,
    loader: Loader<K, V, E, F>,
}

impl<K, V, E, F> Future for LoadFuture<K, V, E, F>
where
    E: Clone,
    F: BatchFn<K, V, Error = E>,
{
    type Output = Result<V, LoadError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let state = self.loader.state.clone();
        let mut st = state.lock().unwrap();

        // If value for our key is loaded already, then return it immediately.
        if st.loaded.contains_key(&self.id) {
            self.stage = Stage::Finished;
            return Poll::Ready(st.loaded.remove(&self.id).unwrap());
        }

        // If neither our key is enqueued, nor its value is loaded,
        // then it's loading at the moment.
        if !st.queued_keys.contains_key(&self.id) {
            ready!(st.poll_loading(cx));
            self.stage = Stage::Finished;
            return Poll::Ready(st.loaded.remove(&self.id).unwrap());
        }

        // Here is the stage where all enqueued keys should be loaded, as we are
        // deferred enough to be sure that no more keys will be enqueued.
        if st.polls >= self.loader.max_batch_size || st.polls == st.queued_keys.len() {
            if st.loading.is_none() {
                let count = self.loader.max_batch_size.min(st.queued_keys.len());
                st.polls -= count;
                let mut queued_keys = mem::replace(&mut st.queued_keys, HashMap::new());
                let (ids, keys): (Vec<_>, Vec<_>) = st
                    .queue
                    .drain(..count)
                    .map(|id| (id, queued_keys.remove(&id).unwrap()))
                    .unzip();
                mem::replace(&mut st.queued_keys, queued_keys);
                st.loading = Some(LoadingBatch {
                    ids,
                    dropped: HashSet::new(),
                    fut: self.loader.batch_fn.load(&keys),
                })
            }

            ready!(st.poll_loading(cx));
            // Our key can be absent in the batch that was loaded, so we should
            // wait until it will be loaded in next batches.
            if st.loaded.contains_key(&self.id) {
                self.stage = Stage::Finished;
                return Poll::Ready(st.loaded.remove(&self.id).unwrap());
            }
        }

        // We track first attempts to poll Futures of enqueued keys to perform
        // actual loading only after everything queued is polled.
        // This way we can be sure that actual loading is deferred long enough
        // to enqueue all possible keys at this stage of Loader execution
        // before doing actual loading.
        if let Stage::Enqueued = self.stage {
            st.polls += 1;
            self.stage = Stage::Polled;
        }
        st.queued_wakers.insert(self.id, cx.waker().clone());

        // When all possible Futures of enqueued keys have been polled,
        // we reschedule them again to the end of event loop queue.
        // This allows to defer actual loading much better, as leaves space
        // for more event loop ticks to happen before doing actual loading,
        // where new keys may be enqueued.
        if st.polls >= self.loader.max_batch_size || st.polls == st.queue.len() {
            let mut wakers = mem::replace(&mut st.queued_wakers, HashMap::new());
            for i in st.queue.iter().take(self.loader.max_batch_size) {
                if let Some(waker) = wakers.remove(&i) {
                    waker.wake();
                }
            }
            mem::replace(&mut st.queued_wakers, wakers);
        }
        Poll::Pending
    }
}

impl<K, V, E, F> Drop for LoadFuture<K, V, E, F> {
    fn drop(&mut self) {
        if let Stage::Finished = self.stage {
            return;
        }

        let state = self.loader.state.clone();
        let mut st = state.lock().unwrap();

        if st.queued_keys.contains_key(&self.id) {
            if let Stage::Polled = self.stage {
                st.polls -= 1;
            }
            st.queued_keys.remove(&self.id);
            st.queued_wakers.remove(&self.id);
            st.queue.retain(|id| id != &self.id);
        } else if st.loaded.contains_key(&self.id) {
            st.loaded.remove(&self.id);
        } else {
            let mut drop_loading_batch = false;
            if let Some(ref mut batch) = st.loading {
                batch.dropped.insert(self.id);
                drop_loading_batch = batch.dropped.len() == batch.ids.len();
            }
            // If everything in a loading batch has been dropped,
            // then we can drop the whole batch eagerly.
            if drop_loading_batch {
                st.loading = None;
            }
        }
    }
}

enum Stage {
    Enqueued,
    Polled,
    Finished,
}

struct State<K, V, F> {
    polls: usize,
    last_index: usize,
    queue: VecDeque<usize>,
    queued_keys: HashMap<usize, K>,
    queued_wakers: HashMap<usize, Waker>,
    loading: Option<LoadingBatch<F>>,
    loaded: HashMap<usize, V>,
}

impl<K, V, E, F> State<K, Result<V, LoadError<E>>, F>
where
    E: Clone,
    F: Future<Output = Result<Vec<V>, E>> + Unpin,
{
    fn poll_loading(&mut self, cx: &mut Context) -> Poll<()> {
        let result = ready!(Pin::new(&mut self.loading.as_mut().unwrap().fut).poll(cx));
        let ids = mem::replace(&mut self.loading.as_mut().unwrap().ids, vec![]);
        let dropped = mem::replace(&mut self.loading.as_mut().unwrap().dropped, HashSet::new());
        match result {
            Ok(vals) => {
                if vals.len() != ids.len() {
                    let err = LoadError::UnequalKeyValueSize {
                        key_count: ids.len(),
                        value_count: vals.len(),
                    };
                    for id in ids {
                        if !dropped.contains(&id) {
                            self.loaded.insert(id, Err(err.clone()));
                        }
                    }
                } else {
                    for (id, v) in ids.into_iter().zip(vals.into_iter()) {
                        if !dropped.contains(&id) {
                            self.loaded.insert(id, Ok(v));
                        }
                    }
                }
            }
            Err(err) => {
                for id in ids {
                    if !dropped.contains(&id) {
                        self.loaded.insert(id, Err(LoadError::BatchFn(err.clone())));
                    }
                }
            }
        }
        self.loading = None;
        Poll::Ready(())
    }
}

struct LoadingBatch<F> {
    ids: Vec<usize>,
    dropped: HashSet<usize>,
    fut: F,
}
