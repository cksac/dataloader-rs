use async_trait::async_trait;
use dataloader::non_cached::Loader;
use dataloader::BatchFn;
use futures::executor::block_on;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;

struct MyLoadFn;

#[async_trait]
impl BatchFn<usize, usize> for MyLoadFn {
    async fn load(&self, keys: &[usize]) -> HashMap<usize, usize> {
        keys.iter()
            .map(|v| (v.clone(), v.clone()))
            .collect::<HashMap<_, _>>()
    }
}

#[derive(Clone)]
struct Object(usize);

#[async_trait]
impl BatchFn<usize, Object> for MyLoadFn {
    async fn load(&self, keys: &[usize]) -> HashMap<usize, Object> {
        keys.iter()
            .map(|v| (v.clone(), Object(v.clone())))
            .collect::<HashMap<_, _>>()
    }
}

#[test]
fn assert_kinds() {
    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}
    fn _assert_clone<T: Clone>() {}
    _assert_send::<Loader<usize, usize, MyLoadFn>>();
    _assert_sync::<Loader<usize, usize, MyLoadFn>>();
    _assert_clone::<Loader<usize, usize, MyLoadFn>>();

    _assert_send::<Loader<usize, Object, MyLoadFn>>();
    _assert_sync::<Loader<usize, Object, MyLoadFn>>();
    _assert_clone::<Loader<usize, Object, MyLoadFn>>();
}

#[derive(Clone)]
struct LoadFnWithHistory {
    max_batch_loaded: Arc<Mutex<usize>>,
}

#[async_trait]
impl BatchFn<usize, usize> for LoadFnWithHistory {
    async fn load(&self, keys: &[usize]) -> HashMap<usize, usize> {
        // println!("BatchFn load keys {:?}", keys);
        let mut max_batch_loaded = self.max_batch_loaded.lock().unwrap();
        if keys.len() > *max_batch_loaded {
            *max_batch_loaded = keys.len();
        }
        keys.iter()
            .map(|v| (v.clone(), v.clone()))
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
        let loader = Loader::new(load_fn.clone()).with_max_batch_size(4);

        let l1 = loader.clone();
        let h1 = thread::spawn(move || {
            let r1 = l1.load(1);
            let r2 = l1.load(2);
            let r3 = l1.load(3);
            let r4 = l1.load_many(vec![2, 3, 4, 5, 6, 7, 8]);
            let f = futures::future::join4(r1, r2, r3, r4);
            let fv = block_on(f);
            let (_v1, _v2, _v3, v4) = fv;
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
            let f = futures::future::join4(r1, r2, r3, r4);
            let _fv = block_on(f);
        });

        let l3 = loader.clone();
        let h3 = thread::spawn(move || {
            let r1 = l3.load_many(vec![12, 13, 14, 1, 2, 3, 4]);
            let r2 = l3.load_many(vec![1, 2, 3, 4, 5, 6, 7]);
            let r3 = l3.load_many(vec![9, 10, 11, 12, 13, 14]);
            let f = futures::future::join3(r1, r2, r3);
            let fv = block_on(f);

            let (v1, v2, v3) = fv;
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

        let max_batch_size = loader.max_batch_size();
        let max_batch_loaded = load_fn.max_batch_loaded.lock().unwrap();
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
        let loader = Loader::new(load_fn.clone()).with_max_batch_size(4);
        let r = loader.load_many(vec![2, 3, 4, 5, 6, 7, 8]);
        let _fv = block_on(r);
        i += 1;

        let max_batch_size = loader.max_batch_size();
        let max_batch_loaded = load_fn.max_batch_loaded.lock().unwrap();
        assert!(*max_batch_loaded > 1);
        assert!(
            *max_batch_loaded <= max_batch_size,
            "max_batch_loaded({}) <= max_batch_size({})",
            *max_batch_loaded,
            max_batch_size
        );
    }
}
