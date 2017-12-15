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
        Box::new(ok(keys.into_iter().map(|v| v * 10).collect()))
    }
}

fn main() {
    let loader = Loader::new(Batcher);
    println!("\n -- Using Loader --");
    {
        let v1 = loader.load(3).and_then(|v| loader.load_many(vec![v, v + 5, v + 10]));
        let v2 = loader.load(4).and_then(|v| loader.load_many(vec![v, v + 5, v + 10]));
        let all = v1.join(v2);
        let output = all.wait().unwrap();
        let expected = (vec![300, 350, 400], vec![400, 450, 500]);
        assert_eq!(expected, output);
    }

    let ld = loader.cached();
    println!("\n -- Using Cached Loader --");
    {
        let v1 = ld.load(3).and_then(|v| ld.load_many(vec![v, v + 5, v + 10]));
        let v2 = ld.load(4).and_then(|v| ld.load_many(vec![v, v + 5, v + 10]));
        let all = v1.join(v2);
        let output = all.wait().unwrap();
        let expected = (vec![300, 350, 400], vec![400, 450, 500]);
        assert_eq!(expected, output);
    }
}