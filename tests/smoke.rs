extern crate futures;
extern crate dataloader;

use dataloader::{Loader, BatchFn, LoadError};
use futures::Future;
use futures::future::ok;

struct Batcher;
impl BatchFn<i32, i32> for Batcher {
    type Error = LoadError;
    fn load(&self, keys: &Vec<i32>) -> Box<Future<Item = Vec<i32>, Error = Self::Error>> {
        //println!("{:?}", keys);
        ok(keys.into_iter().map(|v| v * 10).collect()).boxed()
    }

    fn max_batch_size(&self) -> usize {
        1
    }
}


#[test]
fn smoke() {
    let loader = Loader::new(Batcher);
    let v1 = loader.load(1);
    let v2 = loader.load(2);
    assert_eq!((10, 20), v1.join(v2).wait().unwrap());
}

#[test]
fn smoke_nested() {
    let loader = Loader::new(Batcher);
    let v1 = loader.load(3).map(|v| loader.load(v).wait().unwrap());
    assert_eq!(300, v1.wait().unwrap());
}