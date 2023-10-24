# Dataloader

![Rust](https://github.com/cksac/dataloader-rs/workflows/Rust/badge.svg)
[![Crates.io](https://img.shields.io/crates/v/dataloader.svg)](https://crates.io/crates/dataloader)

Rust implementation of [Facebook's DataLoader](https://github.com/facebook/dataloader) using async-await.

[Documentation](https://docs.rs/dataloader)

## Features
* [x] Batching load requests with caching
* [x] Batching load requests without caching

## Usage
### Switching runtime, by using cargo features
- `runtime-async-std` (default), to use the [async-std](https://async.rs) runtime
    - dataloader = "0.17"
- `runtime-tokio` to use the [Tokio](https://tokio.rs) runtime
    - dataloader = { version = "0.17", default-features = false, features = ["runtime-tokio"]}


### Add to your `Cargo.toml`:
```toml
[dependencies]
dataloader = "0.17"
futures = "0.3"
```

### Example:
```rust
use dataloader::cached::Loader;
use dataloader::BatchFn;
use futures::executor::block_on;
use futures::future::ready;
use std::collections::HashMap;
use std::thread;

struct MyLoadFn;

impl BatchFn<usize, usize> for MyLoadFn {
    async fn load(&mut self, keys: &[usize]) -> HashMap<usize, usize> {
        println!("BatchFn load keys {:?}", keys);
        let ret = keys.iter()
            .map(|v| (v.clone(), v.clone()))
            .collect::<HashMap<_, _>>();
        ready(ret).await
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
```

# LICENSE

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   http://opensource.org/licenses/MIT)

at your option.