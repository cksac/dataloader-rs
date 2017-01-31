extern crate futures;
extern crate dataloader;

use dataloader::{Loader, BatchFn, BatchFuture, LoadError};
use futures::Future;
use futures::future::{err, ok};

struct Batcher;
impl BatchFn<i32, i32> for Batcher {
    type Error = ();
    fn load(&self, keys: &[i32]) -> BatchFuture<i32, Self::Error> {
        // println!("load batch {:?}", keys);
        ok(keys.into_iter().map(|v| v * 10).collect()).boxed()
    }

    fn max_batch_size(&self) -> usize {
        2
    }
}

#[derive(Clone, Debug, PartialEq)]
enum MyError {
    Unknown,
}

struct BadBatcher;
impl BatchFn<i32, i32> for BadBatcher {
    type Error = MyError;
    fn load(&self, _keys: &[i32]) -> BatchFuture<i32, Self::Error> {
        // fail whole batch
        err(MyError::Unknown).boxed()
    }
}

#[derive(Clone, Debug, PartialEq)]
enum ValueError {
    NotEven,
}

impl BatchFn<i32, Result<i32, ValueError>> for BadBatcher {
    type Error = MyError;
    fn load(&self, keys: &[i32]) -> BatchFuture<Result<i32, ValueError>, Self::Error> {
        ok(keys.into_iter()
                .map(|v| if v % 2 == 0 {
                    Ok(v * 10)
                } else {
                    Err(ValueError::NotEven)
                })
                .collect())
            .boxed()
    }
}

#[test]
fn smoke() {
    let loader = Loader::new(Batcher);
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
    let loader = Loader::new(Batcher);
    let v1 = loader.load(3).map(|v| loader.load(v).wait().unwrap());
    let v2 = loader.load(4).map(|v| loader.load(v).wait().unwrap());
    assert_eq!((300, 400), v1.join(v2).wait().unwrap());
}

#[test]
fn nested_load_many() {
    let loader = Loader::new(Batcher);
    let v1 = loader.load(3).map(|v| loader.load_many(vec![v, v + 1, v + 2]).wait().unwrap());
    let v2 = loader.load(4).map(|v| loader.load_many(vec![v, v + 1, v + 2]).wait().unwrap());
    let expected = (vec![300, 310, 320], vec![400, 410, 420]);
    assert_eq!(expected, v1.join(v2).wait().unwrap());
}

#[test]
fn test_batch_fn_error() {
    let loader = Loader::<i32, i32, MyError>::new(BadBatcher);
    let v1 = loader.load(1).wait();
    assert_eq!(LoadError::BatchFn(MyError::Unknown), v1.err().unwrap());
}

#[test]
fn test_result_val() {
    let loader = Loader::<i32, Result<i32, ValueError>, MyError>::new(BadBatcher);
    let v1 = loader.load_many(vec![1, 2]).wait();
    assert_eq!(vec![Err(ValueError::NotEven), Ok(20)], v1.unwrap());
}
