extern crate futures;
extern crate dataloader;

use dataloader::{Loader, BatchFn, BatchFuture};
use futures::Future;
use futures::future::ok;

struct Batcher;
impl BatchFn<i32, i32> for Batcher {
    type Error = ();

    fn load(&self, keys: &[i32]) -> BatchFuture<i32, Self::Error> {
        println!("load batch {:?}", keys);
        ok(keys.into_iter().map(|v| v * 10).collect()).boxed()
    }
}

fn main() {
    let loader = Loader::new(Batcher);
    let v1 = loader.load(1);
    let v2 = loader.load(2);
    assert_eq!((10, 20), v1.join(v2).wait().unwrap());

    let many = loader.load_many(vec![10, 20, 30]);
    assert_eq!(vec![100, 200, 300], many.wait().unwrap());

    let loader_ref = &loader;
    {
        let v1 = loader_ref.load(3)
            .map(|v| loader_ref.load_many(vec![v, v + 1, v + 2]).wait().unwrap());
        let v2 = loader_ref.load(4)
            .map(|v| loader_ref.load_many(vec![v, v + 1, v + 2]).wait().unwrap());

        let expected = (vec![300, 310, 320], vec![400, 410, 420]);
        assert_eq!(expected, v1.join(v2).wait().unwrap());
    }
}