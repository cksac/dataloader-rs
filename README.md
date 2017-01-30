# Dataloader
[![Build Status](https://travis-ci.org/cksac/dataloader-rs.svg?branch=master)](https://travis-ci.org/cksac/dataloader-rs)
[![Crates.io](https://img.shields.io/crates/v/dataloader.svg)](https://crates.io/crates/dataloader)

Rust implementation of [Facebook's DataLoader](https://github.com/facebook/dataloader) using futures and tokio-core.

[Documentation](https://docs.rs/dataloader)

## Status
This project is a work in progress.
 - [x] Batching load requests
 - [ ] Cache load result

## Usage
Add to your Cargo.toml
```toml
[dependencies]
futures = "0.1"
dataloader = "0.1"
```

Add to your crate
```rust
extern crate futures;
extern crate dataloader;

use dataloader::{Loader, BatchFn, LoadError};
use futures::Future;
use futures::future::ok;

struct Batcher;
impl BatchFn<i32, i32> for Batcher {
    type Error = LoadError;
    fn load(&self, keys: &[i32]) -> Box<Future<Item = Vec<i32>, Error = Self::Error>> {
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
        let v1 = loader_ref.load(3).map(|v| loader_ref.load_many(vec![v, v + 1, v + 2]).wait().unwrap());
        let v2 = loader_ref.load(4).map(|v| loader_ref.load_many(vec![v, v + 1, v + 2]).wait().unwrap());
        let expected = (vec![300, 310, 320], vec![400, 410, 420]);
        assert_eq!(expected, v1.join(v2).wait().unwrap());
    }
}
```