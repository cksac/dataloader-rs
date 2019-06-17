use std::{collections::BTreeMap, thread};

use futures::{executor, future, TryFutureExt as _};

use super::*;

#[test]
fn assert_kinds() {
    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}
    fn _assert_clone<T: Clone>() {}
    _assert_send::<
        cached::Loader<i32, i32, (), Batcher, BTreeMap<i32, cached::Item<i32, i32, (), Batcher>>>,
    >();
    _assert_sync::<
        cached::Loader<i32, i32, (), Batcher, BTreeMap<i32, cached::Item<i32, i32, (), Batcher>>>,
    >();
    _assert_clone::<
        cached::Loader<i32, i32, (), Batcher, BTreeMap<i32, cached::Item<i32, i32, (), Batcher>>>,
    >();
}

#[test]
fn smoke() {
    let loader = Loader::new(Batcher::new(2)).cached();

    let v = future::try_join3(loader.load(1), loader.load(2), loader.load(3));
    assert_eq!((10, 20, 30), executor::block_on(v).unwrap());

    let v = loader.load_many(vec![10, 20, 30]);
    assert_eq!(vec![100, 200, 300], executor::block_on(v).unwrap());

    let loader_ref = &loader;
    {
        let v = future::try_join(loader_ref.load(1), loader_ref.load(2));
        assert_eq!((10, 20), executor::block_on(v).unwrap());
    }
    {
        let v1 = loader_ref.load(3).and_then(|v| loader_ref.load(v));
        let v2 = loader_ref.load(4).and_then(|v| loader_ref.load(v));
        let all = future::try_join(v1, v2);
        assert_eq!((300, 400), executor::block_on(all).unwrap());
    }
}

#[test]
fn drop_loader() {
    let all = {
        let loader = Loader::new(Batcher::new(10)).cached();
        let v1 = loader.load(1);
        let v2 = loader.load(2);
        drop(loader);
        future::try_join(v1, v2)
    };
    assert_eq!((10, 20), executor::block_on(all).unwrap());
}

#[test]
fn dispatch_partial_batch() {
    let loader = Loader::new(Batcher::new(10)).cached();
    let all = future::try_join(loader.load(1), loader.load(2));
    assert_eq!((10, 20), executor::block_on(all).unwrap());
}

#[test]
fn nested_load() {
    let loader = Loader::new(Batcher::new(2)).cached();
    let v1 = loader.load(3).and_then(|v| loader.load(v));
    let v2 = loader.load(4).and_then(|v| loader.load(v));
    let all = future::try_join(v1, v2);
    assert_eq!((300, 400), executor::block_on(all).unwrap());
}

#[test]
fn nested_load_many() {
    let loader = Loader::new(Batcher::new(2)).cached();
    let v1 = loader
        .load(3)
        .and_then(|v| loader.load_many(vec![v, v + 1, v + 2]));
    let v2 = loader
        .load(4)
        .and_then(|v| loader.load_many(vec![v, v + 1, v + 2]));
    let expected = (vec![300, 310, 320], vec![400, 410, 420]);
    let all = future::try_join(v1, v2);
    assert_eq!(expected, executor::block_on(all).unwrap());
}

#[test]
fn test_batch_fn_error() {
    let loader = Loader::<i32, i32, MyError, BadBatcher>::new(BadBatcher).cached();
    let v1 = executor::block_on(loader.load(1));
    assert_eq!(LoadError::BatchFn(MyError::Unknown), v1.err().unwrap());
}

#[test]
fn test_result_val() {
    let loader =
        Loader::<i32, Result<i32, ValueError>, MyError, BadBatcher>::new(BadBatcher).cached();
    let v1 = executor::block_on(loader.load_many(vec![1, 2]));
    assert_eq!(vec![Err(ValueError::NotEven), Ok(20)], v1.unwrap());
}

#[test]
fn test_batch_call_seq() {
    // batch size = 2, value will be (batch_fn call seq, v * 10)
    let loader = Loader::<i32, (usize, i32), (), _>::new(Batcher::new(2)).cached();

    let v1 = loader.load(1);
    let v2 = loader.load(2);
    //v1 and v2 should be in first batch
    assert_eq!(
        ((1, 10), (1, 20)),
        executor::block_on(future::try_join(v1, v2)).unwrap(),
    );

    let v3 = loader.load(3);
    let v4 = loader.load(4);
    //v3 and v4 should be in second batch
    assert_eq!(
        ((2, 30), (2, 40)),
        executor::block_on(future::try_join(v3, v4)).unwrap(),
    );

    let v5 = loader.load(1);
    let v6 = loader.load(2);
    //v5 and v6 should be using cache of first batch
    assert_eq!(
        ((1, 10), (1, 20)),
        executor::block_on(future::try_join(v5, v6)).unwrap(),
    );
}

#[test]
fn pass_to_thread() {
    let loader = Loader::new(Batcher::new(4)).cached();

    let l1 = loader.clone();
    let h1 = thread::spawn(move || {
        let all = future::try_join(l1.load(1), l1.load(2));
        executor::block_on(all).unwrap()
    });

    let l2 = loader.clone();
    let h2 = thread::spawn(move || {
        let all = future::try_join(l2.load(1), l2.load(2));
        executor::block_on(all).unwrap()
    });

    assert_eq!((10, 20), h1.join().unwrap());
    assert_eq!((10, 20), h2.join().unwrap());
}

#[test]
fn test_run_by_threadpool() {
    let mut rt = executor::ThreadPool::new().unwrap();

    let loader = Loader::new(Batcher::new(10)).cached();
    let v1 = loader
        .load(3)
        .and_then(|v| loader.load_many(vec![v, v + 1, v + 2]));
    let v2 = loader
        .load(4)
        .and_then(|v| loader.load_many(vec![v, v + 1, v + 2]));
    assert_eq!(
        (vec![300, 310, 320], vec![400, 410, 420]),
        rt.run(future::try_join(v1, v2)).unwrap(),
    );
}

#[test]
fn test_clear() {
    // batch size = 2, value will be (batch_fn call seq, v * 10)
    let loader = Loader::<i32, (usize, i32), (), _>::new(Batcher::new(2)).cached();
    let v1 = loader.load(1);
    let v2 = loader.load(1);
    assert_eq!(
        ((1, 10), (1, 10)),
        executor::block_on(future::try_join(v1, v2)).unwrap(),
    );

    loader.remove(&1);
    assert_eq!((2, 10), executor::block_on(loader.load(1)).unwrap());
}

#[test]
fn test_clear_all() {
    // batch size = 2, value will be (batch_fn call seq, v * 10)
    let loader = Loader::<i32, (usize, i32), (), _>::new(Batcher::new(2)).cached();
    let v1 = loader.load(1);
    let v2 = loader.load(2);
    assert_eq!(
        ((1, 10), (1, 20)),
        executor::block_on(future::try_join(v1, v2)).unwrap(),
    );

    loader.clear();
    let v3 = loader.load(1);
    let v4 = loader.load(2);
    assert_eq!(
        ((2, 10), (2, 20)),
        executor::block_on(future::try_join(v3, v4)).unwrap(),
    );
}

#[test]
fn test_prime() {
    // batch size = 1, value will be (batch_fn call seq, v * 10)
    let loader = Loader::<i32, (usize, i32), (), _>::new(Batcher::new(1)).cached();
    loader.prime(1, (0, 101));
    let v1 = loader.load(1);
    let v2 = loader.load(2);
    assert_eq!((0, 101), executor::block_on(v1).unwrap());
    assert_eq!((1, 20), executor::block_on(v2).unwrap());

    loader.prime(2, (0, 201)); // should have no effect as key 2 are loaded already
    let v3 = loader.load(2);
    assert_eq!((1, 20), executor::block_on(v3).unwrap());
}

#[test]
fn test_custom_cache() {
    // batch size = 2, value will be (batch_fn call seq, v * 10)
    let loader =
        Loader::<i32, (usize, i32), (), _>::new(Batcher::new(2)).with_cache(MyCache::new());
    let v1 = loader.load(1);
    let v2 = loader.load(2);
    assert_eq!(
        ((1, 10), (1, 20)),
        executor::block_on(future::try_join(v1, v2)).unwrap(),
    );

    loader.clear();
    let v3 = loader.load(1);
    let v4 = loader.load(2);
    assert_eq!(
        ((2, 10), (2, 20)),
        executor::block_on(future::try_join(v3, v4)).unwrap(),
    );
}

#[test]
fn single_load_for_key_concurrently() {
    // batch size = 2, value will be (batch_fn call seq, v * 10)
    let loader = Loader::<i32, (usize, i32), (), _>::new(Batcher::new(2)).cached();
    let all = future::try_join3(loader.load(1), loader.load(1), loader.load(1));
    let expected = ((1, 10), (1, 10), (1, 10));
    assert_eq!(expected, executor::block_on(all).unwrap());
}

#[test]
fn single_load_for_key_sequentially() {
    // batch size = 2, value will be (batch_fn call seq, v * 10)
    let loader = Loader::<i32, (usize, i32), (), _>::new(Batcher::new(2)).cached();
    let all = loader
        .load(1)
        .and_then(|_| loader.load(1))
        .and_then(|_| loader.load(1));
    assert_eq!((1, 10), executor::block_on(all).unwrap());
}
