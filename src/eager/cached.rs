use crate::BatchFn;
use async_std::sync::{Arc, Mutex};
use async_std::task;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

pub struct Loader<K, V, E, F>
where
    K: Eq + Hash + Clone,
    V: Clone,
    E: Clone,
    F: BatchFn<K, V, Error = E>,
{
    completed: Arc<Mutex<HashMap<K, Result<V, E>>>>,
    pending: Arc<Mutex<HashSet<K>>>,
    load_fn: Arc<Mutex<F>>,
    max_batch_size: usize,
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
            completed: self.completed.clone(),
            pending: self.pending.clone(),
            max_batch_size: self.max_batch_size.clone(),
            load_fn: self.load_fn.clone(),
        }
    }
}

impl<K, V, E, F> Loader<K, V, E, F>
where
    K: Eq + Hash + Clone,
    V: Clone,
    E: Clone,
    F: BatchFn<K, V, Error = E>,
{
    pub fn new(load_fn: F) -> Loader<K, V, E, F> {
        Loader {
            completed: Arc::new(Mutex::new(HashMap::new())),
            pending: Arc::new(Mutex::new(HashSet::new())),
            max_batch_size: load_fn.max_batch_size(),
            load_fn: Arc::new(Mutex::new(load_fn)),
        }
    }

    pub async fn load(&self, key: K) -> Result<V, F::Error> {
        let completed = self.completed.lock().await;
        if let Some(v) = completed.get(&key) {
            return (*v).clone();
        }
        drop(completed);

        let mut pending = self.pending.lock().await;
        let completed = self.completed.lock().await;
        if let Some(v) = completed.get(&key) {
            return (*v).clone();
        }
        drop(completed);
        if pending.get(&key).is_none() {
            pending.insert(key.clone());
            if pending.len() >= self.max_batch_size {
                let keys = pending.drain().collect::<Vec<K>>();
                let load_fn = self.load_fn.lock().await;
                let load_ret = load_fn.load(keys.as_ref()).await;
                let mut completed = self.completed.lock().await;
                for (k, v) in load_ret.into_iter() {
                    completed.insert(k, v);
                }
                return completed
                    .get(&key)
                    .cloned()
                    .expect("found result in completed");
            }
        }
        drop(pending);
        task::yield_now().await;

        let mut pending = self.pending.lock().await;
        let completed = self.completed.lock().await;
        if let Some(v) = completed.get(&key) {
            return (*v).clone();
        }
        drop(completed);
        let keys = pending.drain().collect::<Vec<K>>();

        let load_fn = self.load_fn.lock().await;
        let load_ret = load_fn.load(keys.as_ref()).await;
        let mut completed = self.completed.lock().await;
        for (k, v) in load_ret.into_iter() {
            completed.insert(k, v);
        }
        return completed
            .get(&key)
            .cloned()
            .expect("found result in completed");
    }

    pub async fn load_many(&self, keys: Vec<K>) -> HashMap<K, Result<V, F::Error>> {
        let mut ret = HashMap::new();
        let mut rest = HashSet::new();
        for key in keys.into_iter() {
            let completed = self.completed.lock().await;
            if let Some(v) = completed.get(&key) {
                ret.insert(key, (*v).clone());
                continue;
            }
            drop(completed);

            let mut pending = self.pending.lock().await;
            let completed = self.completed.lock().await;
            if let Some(v) = completed.get(&key) {
                ret.insert(key, (*v).clone());
                continue;
            }
            drop(completed);
            if pending.get(&key).is_none() {
                pending.insert(key.clone());
                if pending.len() >= self.max_batch_size {
                    let keys = pending.drain().collect::<Vec<K>>();
                    if keys.len() > 0 {
                        let load_fn = self.load_fn.lock().await;
                        let load_ret = load_fn.load(keys.as_ref()).await;
                        let mut completed = self.completed.lock().await;
                        for (k, v) in load_ret.into_iter() {
                            completed.insert(k, v);
                        }
                        let v = completed
                            .get(&key)
                            .cloned()
                            .expect("found result in completed");
                        ret.insert(key, v);
                        continue;
                    }
                }
            }
            drop(pending);
            rest.insert(key);
        }

        task::yield_now().await;
        let mut pending = self.pending.lock().await;
        let mut completed = self.completed.lock().await;
        let keys = pending.drain().collect::<Vec<K>>();
        if keys.len() > 0 {
            let load_fn = self.load_fn.lock().await;
            let load_ret = load_fn.load(keys.as_ref()).await;
            for (k, v) in load_ret.into_iter() {
                completed.insert(k, v);
            }
        }
        for key in rest.into_iter() {
            if let Some(v) = completed.get(&key) {
                ret.insert(key, (*v).clone());
            }
        }
        ret
    }
}
