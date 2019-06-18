use dataloader::{BatchFn, BatchFuture, Loader};
use futures::{future, FutureExt as _, TryFutureExt as _};

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
    let mut rt = tokio::runtime::Runtime::new().unwrap();

    let loader = Loader::new(Batcher);
    println!("\n -- Using Loader --");
    {
        let loader1 = loader.clone();
        let loader2 = loader.clone();
        let v1 = loader1
            .load(3)
            .and_then(move |v| loader1.load_many(vec![v, v + 5, v + 10]));
        let v2 = loader2
            .load(4)
            .and_then(move |v| loader2.load_many(vec![v, v + 5, v + 10]));
        let all = future::try_join(v1, v2);
        let output = rt.block_on(all.boxed().compat()).unwrap();
        let expected = (vec![300, 350, 400], vec![400, 450, 500]);
        assert_eq!(expected, output);
    }

    let ld = loader.cached();
    println!("\n -- Using Cached Loader --");
    {
        let ld1 = ld.clone();
        let ld2 = ld.clone();
        let v1 = ld1
            .load(3)
            .and_then(move |v| ld1.load_many(vec![v, v + 5, v + 10]));
        let v2 = ld2
            .load(4)
            .and_then(move |v| ld2.load_many(vec![v, v + 5, v + 10]));
        let all = future::try_join(v1, v2);
        let output = rt.block_on(all.boxed().compat()).unwrap();
        let expected = (vec![300, 350, 400], vec![400, 450, 500]);
        assert_eq!(expected, output);
    }
}
