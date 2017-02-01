extern crate futures;
extern crate dataloader;
extern crate tokio_core;

use dataloader::{Loader, LoadError};

use futures::Future;
use tokio_core::reactor::Core;

mod common;
use common::batcher::*;

#[test]
fn smoke() {
    let loader = Loader::new(Batcher::new(2)).cached();
    let v1 = loader.load(1);
    let v2 = loader.load(2);
    let v3 = loader.load(3);
    assert_eq!((10, 20), v1.join(v2).wait().unwrap());
    assert_eq!(30, v3.wait().unwrap());

    let many = loader.load_many(vec![10, 20, 30]);
    assert_eq!(vec![100, 200, 300], many.wait().unwrap());

    let loader_ref = &loader;
    {
        let v1 = loader_ref.load(1);
        let v2 = loader_ref.load(2);
        assert_eq!((10, 20), v1.join(v2).wait().unwrap());
    }
    {
        let v1 = loader_ref.load(3).map(|v| loader_ref.load(v).wait().unwrap());
        let v2 = loader_ref.load(4).map(|v| loader_ref.load(v).wait().unwrap());
        assert_eq!((300, 400), v1.join(v2).wait().unwrap());
    }
}

#[test]
fn nested_load() {
    let loader = Loader::new(Batcher::new(2)).cached();
    let v1 = loader.load(3).map(|v| loader.load(v).wait().unwrap());
    let v2 = loader.load(4).map(|v| loader.load(v).wait().unwrap());
    assert_eq!((300, 400), v1.join(v2).wait().unwrap());
}

#[test]
fn nested_load_many() {
    let loader = Loader::new(Batcher::new(2)).cached();
    let v1 = loader.load(3).map(|v| loader.load_many(vec![v, v + 1, v + 2]).wait().unwrap());
    let v2 = loader.load(4).map(|v| loader.load_many(vec![v, v + 1, v + 2]).wait().unwrap());
    let expected = (vec![300, 310, 320], vec![400, 410, 420]);
    assert_eq!(expected, v1.join(v2).wait().unwrap());
}

#[test]
fn test_batch_fn_error() {
    let loader = Loader::<i32, i32, MyError>::new(BadBatcher).cached();
    let v1 = loader.load(1).wait();
    assert_eq!(LoadError::BatchFn(MyError::Unknown), v1.err().unwrap());
}

#[test]
fn test_result_val() {
    let loader = Loader::<i32, Result<i32, ValueError>, MyError>::new(BadBatcher).cached();
    let v1 = loader.load_many(vec![1, 2]).wait();
    assert_eq!(vec![Err(ValueError::NotEven), Ok(20)], v1.unwrap());
}

#[test]
fn test_batch_call_seq() {
    // batch size = 2, value will be (batch_fn call seq,  v * 10)
    let loader = Loader::<i32, (usize, i32), ()>::new(Batcher::new(2)).cached();
    let v1 = loader.load(1);
    let v2 = loader.load(2);
    let v3 = loader.load(3);
    let v4 = loader.load(4);
    let v5 = loader.load(1);
    let v6 = loader.load(2);

    //v1 and v2 should be in first batch
    assert_eq!((1, 10), v1.wait().unwrap());
    assert_eq!((1, 20), v2.wait().unwrap());
    //v3 and v4 should be in sencod batch
    assert_eq!((2, 30), v3.wait().unwrap());
    assert_eq!((2, 40), v4.wait().unwrap());
    //v5 and v6 should be using cache of first batch
    assert_eq!((1, 10), v5.wait().unwrap());
    assert_eq!((1, 20), v6.wait().unwrap());
}

#[test]
fn pass_to_thread() {
    use std::thread;

    let loader = Loader::new(Batcher::new(2)).cached();
    let l = loader.clone();
    let h = thread::spawn(move || {
        let v1 = l.load(1);
        let v2 = l.load(2);
        assert_eq!((10, 20), v1.join(v2).wait().unwrap());
    });
    let _ = h.join();
}

#[test]
fn test_run_by_core() {
    let mut core = Core::new().unwrap();
    let loader = Loader::new(Batcher::new(10)).cached();
    let v1 = loader.load(3).and_then(|v| loader.load_many(vec![v, v + 1, v + 2]));
    let v2 = loader.load(4).and_then(|v| loader.load_many(vec![v, v + 1, v + 2]));
    let all = v1.join(v2);
    let output = core.run(all).unwrap();
    let expected = (vec![300, 310, 320], vec![400, 410, 420]);
    assert_eq!(expected, output);
}

#[test]
fn test_clear() {
    // batch size = 1, value will be (batch_fn call seq,  v * 10)
    let loader = Loader::<i32, (usize, i32), ()>::new(Batcher::new(1)).cached();
    let v1 = loader.load(1);
    let v2 = loader.load(1);

    loader.clear(&1);
    let v3 = loader.load(1);

    assert_eq!((1, 10), v1.wait().unwrap());
    assert_eq!((1, 10), v2.wait().unwrap());
    assert_eq!((2, 10), v3.wait().unwrap());
}

#[test]
fn test_clear_all() {
    // batch size = 2, value will be (batch_fn call seq,  v * 10)
    let loader = Loader::<i32, (usize, i32), ()>::new(Batcher::new(2)).cached();
    let v1 = loader.load(1);
    let v2 = loader.load(2);

    loader.clear_all();
    let v3 = loader.load(1);
    let v4 = loader.load(2);

    assert_eq!((1, 10), v1.wait().unwrap());
    assert_eq!((1, 20), v2.wait().unwrap());
    assert_eq!((2, 10), v3.wait().unwrap());
    assert_eq!((2, 20), v4.wait().unwrap());
}

#[test]
fn test_prime() {
    // batch size = 2, value will be (batch_fn call seq,  v * 10)
    let loader = Loader::<i32, (usize, i32), ()>::new(Batcher::new(1)).cached();
    loader.prime(1, (0, 101));
    let v1 = loader.load(1);
    let v2 = loader.load(2);
    loader.prime(2, (0, 201)); // should have no effect as key 2 are loaded alredy
    let v3 = loader.load(2);

    assert_eq!((0, 101), v1.wait().unwrap());
    assert_eq!((1, 20), v2.wait().unwrap());
    assert_eq!((1, 20), v3.wait().unwrap());
}