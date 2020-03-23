# Dataloader

[![Build Status](https://travis-ci.org/cksac/dataloader-rs.svg?branch=master)](https://travis-ci.org/cksac/dataloader-rs)
[![Crates.io](https://img.shields.io/crates/v/dataloader.svg)](https://crates.io/crates/dataloader)
[![Coverage Status](https://coveralls.io/repos/github/cksac/dataloader-rs/badge.svg?branch=master)](https://coveralls.io/github/cksac/dataloader-rs?branch=master)

Rust implementation of [Facebook's DataLoader](https://github.com/facebook/dataloader) using [futures](https://docs.rs/futures-preview).

[Documentation](https://docs.rs/dataloader)

## Features
* Eager loader
    * [x] Batching load requests with caching
    * [ ] Batching load requests without caching
* Lazy loader
    * [ ] Batching load requests with caching
    * [ ] Batching load requests without caching

## Usage

Add to your `Cargo.toml`:
```toml
[dependencies]
dataloader = "0.8"
futures = { version = "0.3" }
async-trait = "0.1"

[dependencies.async-std]
version = "1.0"
features = ["unstable"]
```

example:
```rust
use async_std::prelude::*;
use async_std::task;
use async_trait::async_trait;
use dataloader::eager::cached::Loader;
use dataloader::BatchFn;
use std::collections::HashMap;
use std::thread;

struct MyLoadFn;

#[async_trait]
impl BatchFn<usize, usize> for MyLoadFn {
    type Error = ();

    fn max_batch_size(&self) -> usize {
        4
    }

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
        let loader = Loader::new(a);

        let l1 = loader.clone();
        let h1 = thread::spawn(move || {
            let r1 = l1.load(1);
            let r2 = l1.load(2);
            let r3 = l1.load(3);

            let r4 = l1.load_many(vec![2, 3, 4, 5, 6, 7, 8]);
            let f = r1.join(r2).join(r3).join(r4);
            println!("{:?}", task::block_on(f));
        });

        let l2 = loader.clone();
        let h2 = thread::spawn(move || {
            let r1 = l2.load(1);
            let r2 = l2.load(2);
            let r3 = l2.load(3);
            let r4 = l2.load(4);
            let f = r1.join(r2).join(r3).join(r4);
            println!("{:?}", task::block_on(f));
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