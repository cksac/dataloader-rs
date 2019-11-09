use dataloader::{BatchFn, BatchFuture, Loader};
use futures::{executor, future, FutureExt as _, TryFutureExt as _};

struct Batcher;

impl BatchFn<i32, i32> for Batcher {
    type Error = ();

    fn load(&self, keys: &[i32]) -> BatchFuture<i32, Self::Error> {
        println!("load batch {:?}", keys);
        future::ready(keys.into_iter().map(|v| v * 10).collect())
            .unit_error()
            .boxed()
    }
}

fn main() {
    let mut rt = executor::LocalPool::new();

    let loader = Loader::new(Batcher);
    println!("\n -- Using Loader --");
    {
        let v1 = loader
            .load(3)
            .and_then(|v| loader.load_many(vec![v, v + 5, v + 10]));
        let v2 = loader
            .load(4)
            .and_then(|v| loader.load_many(vec![v, v + 5, v + 10]));
        let output = rt.run_until(future::try_join(v1, v2)).unwrap();
        let expected = (vec![300, 350, 400], vec![400, 450, 500]);
        assert_eq!(expected, output);
    }

    let ld = loader.cached();
    println!("\n -- Using Cached Loader --");
    {
        let v1 = ld
            .load(3)
            .and_then(|v| ld.load_many(vec![v, v + 5, v + 10]));
        let v2 = ld
            .load(4)
            .and_then(|v| ld.load_many(vec![v, v + 5, v + 10]));
        let output = rt.run_until(future::try_join(v1, v2)).unwrap();
        let expected = (vec![300, 350, 400], vec![400, 450, 500]);
        assert_eq!(expected, output);
    }
}
