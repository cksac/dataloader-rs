# Dataloader
[![Build Status](https://travis-ci.org/cksac/dataloader-rs.svg?branch=master)](https://travis-ci.org/cksac/dataloader-rs)
[![Latest Version](https://img.shields.io/crates/v/dataloader.svg)](https://crates.io/crates/dataloader)
Rust implementation of [Facebook's DataLoader](https://github.com/facebook/dataloader) using futures and tokio-core.

## Status
This project is a work in progress.
 - [x] Batching load requests
 - [ ] Cache load result

## Installation
Add fake to your Cargo.toml
```toml
[dependencies]
futures = "0.1"
dataloader = "0.1"
```

## Usage
```rust
extern crate futures;
extern crate dataloader;

use dataloader::{Loader, BatchFn, LoadError};
use futures::Future;
use futures::future::ok;

struct Batcher;
impl BatchFn<i32, i32> for Batcher {
    type Error = LoadError;
    fn load(&self, keys: &Vec<i32>) -> Box<Future<Item = Vec<i32>, Error = Self::Error>> {
        println!("load batch with keys: {:?}", keys);
        ok(keys.into_iter().map(|v| v * 10).collect()).boxed()
    }
}

fn main() {
    let loader = Loader::new(Batcher);
    let v1 = loader.load(1);
    let v2 = loader.load(2);
    assert_eq!((10, 20), v1.join(v2).wait().unwrap());
}
```