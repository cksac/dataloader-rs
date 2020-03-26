use crate::non_cached::Loader;
use crate::BatchFn;
use async_std::prelude::*;
use async_std::sync::{Arc, Mutex};
use async_std::task;
use async_trait::async_trait;
use std::collections::HashMap;
use std::thread;

struct MyLoadFn;

#[async_trait]
impl BatchFn<usize, usize> for MyLoadFn {
    type Error = ();

    fn max_batch_size(&self) -> usize {
        4
    }

    async fn load(&self, keys: &[usize]) -> HashMap<usize, Result<usize, Self::Error>> {
        keys.iter()
            .map(|v| (v.clone(), Ok(v.clone())))
            .collect::<HashMap<_, _>>()
    }
}

#[derive(Clone)]
struct Object(usize);

#[async_trait]
impl BatchFn<usize, Object> for MyLoadFn {
    type Error = ();

    fn max_batch_size(&self) -> usize {
        4
    }

    async fn load(&self, keys: &[usize]) -> HashMap<usize, Result<Object, Self::Error>> {
        keys.iter()
            .map(|v| (v.clone(), Ok(Object(v.clone()))))
            .collect::<HashMap<_, _>>()
    }
}

#[test]
fn assert_kinds() {
    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}
    fn _assert_clone<T: Clone>() {}
    _assert_send::<Loader<usize, usize, (), MyLoadFn>>();
    _assert_sync::<Loader<usize, usize, (), MyLoadFn>>();
    _assert_clone::<Loader<usize, usize, (), MyLoadFn>>();

    _assert_send::<Loader<usize, Object, (), MyLoadFn>>();
    _assert_sync::<Loader<usize, Object, (), MyLoadFn>>();
    _assert_clone::<Loader<usize, Object, (), MyLoadFn>>();
}

#[derive(Clone)]
struct LoadFnWithHistory {
    max_batch_loaded: Arc<Mutex<usize>>,
}

#[async_trait]
impl BatchFn<usize, usize> for LoadFnWithHistory {
    type Error = ();

    fn max_batch_size(&self) -> usize {
        4
    }

    async fn load(&self, keys: &[usize]) -> HashMap<usize, Result<usize, Self::Error>> {
        // println!("BatchFn load keys {:?}", keys);
        let mut max_batch_loaded = self.max_batch_loaded.lock().await;
        if keys.len() > *max_batch_loaded {
            *max_batch_loaded = keys.len();
        }
        keys.iter()
            .map(|v| (v.clone(), Ok(v.clone())))
            .collect::<HashMap<_, _>>()
    }
}

#[test]
fn test_load() {
    let mut i = 0;
    while i < 1000 {
        let load_fn = LoadFnWithHistory {
            max_batch_loaded: Arc::new(Mutex::new(0)),
        };
        let loader = Loader::new(load_fn.clone());

        let l1 = loader.clone();
        let h1 = thread::spawn(move || {
            let r1 = l1.load(1);
            let r2 = l1.load(2);
            let r3 = l1.load(3);

            let r4 = l1.load_many(vec![2, 3, 4, 5, 6, 7, 8]);
            let f = r1.join(r2).join(r3).join(r4);
            let fv = task::block_on(f);
            let (((_v1, _v2), _v3), v4) = fv;
            let mut v4_keys = v4.keys().cloned().collect::<Vec<_>>();
            v4_keys.sort();
            assert_eq!(vec![2, 3, 4, 5, 6, 7, 8], v4_keys);
        });

        let l2 = loader.clone();
        let h2 = thread::spawn(move || {
            let r1 = l2.load(1);
            let r2 = l2.load(2);
            let r3 = l2.load(3);
            let r4 = l2.load(4);
            let f = r1.join(r2).join(r3).join(r4);
            let _fv = task::block_on(f);
        });

        let l3 = loader.clone();
        let h3 = thread::spawn(move || {
            let r1 = l3.load_many(vec![12, 13, 14, 1, 2, 3, 4]);
            let r2 = l3.load_many(vec![1, 2, 3, 4, 5, 6, 7]);
            let r3 = l3.load_many(vec![9, 10, 11, 12, 13, 14]);
            let f = r1.join(r2).join(r3);
            let fv = task::block_on(f);

            let ((v1, v2), v3) = fv;
            let mut v1_keys = v1.keys().cloned().collect::<Vec<_>>();
            v1_keys.sort();
            assert_eq!(vec![1, 2, 3, 4, 12, 13, 14], v1_keys);

            let mut v2_keys = v2.keys().cloned().collect::<Vec<_>>();
            v2_keys.sort();
            assert_eq!(vec![1, 2, 3, 4, 5, 6, 7], v2_keys);

            let mut v3_keys = v3.keys().cloned().collect::<Vec<_>>();
            v3_keys.sort();
            assert_eq!(vec![9, 10, 11, 12, 13, 14], v3_keys);
        });

        h1.join().unwrap();
        h2.join().unwrap();
        h3.join().unwrap();
        i += 1;

        let max_batch_size = load_fn.max_batch_size();
        let max_batch_loaded = task::block_on(load_fn.max_batch_loaded.lock());
        assert!(*max_batch_loaded > 1);
        assert!(
            *max_batch_loaded <= max_batch_size,
            "max_batch_loaded({}) <= max_batch_size({})",
            *max_batch_loaded,
            max_batch_size
        );
    }
}

#[test]
fn test_load_many() {
    let mut i = 0;
    while i < 10 {
        let load_fn = LoadFnWithHistory {
            max_batch_loaded: Arc::new(Mutex::new(0)),
        };
        let loader = Loader::new(load_fn.clone());
        let r = loader.load_many(vec![2, 3, 4, 5, 6, 7, 8]);
        let _fv = task::block_on(r);
        i += 1;

        let max_batch_size = load_fn.max_batch_size();
        let max_batch_loaded = task::block_on(load_fn.max_batch_loaded.lock());
        assert!(*max_batch_loaded > 1);
        assert!(
            *max_batch_loaded <= max_batch_size,
            "max_batch_loaded({}) <= max_batch_size({})",
            *max_batch_loaded,
            max_batch_size
        );
    }
}
