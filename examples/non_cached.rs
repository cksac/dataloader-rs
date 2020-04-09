use async_trait::async_trait;
use dataloader::non_cached::Loader;
use dataloader::BatchFn;
use futures::executor::block_on;
use std::collections::HashMap;
use std::thread;

struct MyLoadFn;

#[async_trait]
impl BatchFn<usize, usize> for MyLoadFn {
    type Error = ();

    async fn load(&self, keys: &[usize]) -> HashMap<usize, Result<usize, Self::Error>> {
        println!("BatchFn load keys {:?}", keys);
        keys.iter()
            .map(|v| (v.clone(), Ok(v.clone())))
            .collect::<HashMap<_, _>>()
    }
}

fn main() {
    let mut i = 0;
    while i < 2 {
        let a = MyLoadFn;
        let loader = Loader::new(a).with_max_batch_size(4);

        let l1 = loader.clone();
        let h1 = thread::spawn(move || {
            let r1 = l1.load(1);
            let r2 = l1.load(2);
            let r3 = l1.load(3);

            let r4 = l1.load_many(vec![2, 3, 4, 5, 6, 7, 8]);
            let f = futures::future::join4(r1, r2, r3, r4);
            println!("{:?}", block_on(f));
        });

        let l2 = loader.clone();
        let h2 = thread::spawn(move || {
            let r1 = l2.load(1);
            let r2 = l2.load(2);
            let r3 = l2.load(3);
            let r4 = l2.load(4);
            let f = futures::future::join4(r1, r2, r3, r4);
            println!("{:?}", block_on(f));
        });

        h1.join().unwrap();
        h2.join().unwrap();
        i += 1;
    }
}
