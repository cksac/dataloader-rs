# Dataloader
[![Build Status](https://travis-ci.org/cksac/dataloader-rs.svg?branch=master)](https://travis-ci.org/cksac/dataloader-rs)
[![Crates.io](https://img.shields.io/crates/v/dataloader.svg)](https://crates.io/crates/dataloader)
[![Coverage Status](https://coveralls.io/repos/github/cksac/dataloader-rs/badge.svg?branch=master)](https://coveralls.io/github/cksac/dataloader-rs?branch=master)

Rust implementation of [Facebook's DataLoader](https://github.com/facebook/dataloader) using futures and tokio-core.

[Documentation](https://docs.rs/dataloader)

## Features
 - [x] Batching load requests
 - [x] Caching load results

## Usage
Add to your Cargo.toml

```toml
[dependencies]
futures = "0.1"
dataloader = "0.3"
```

Add to your crate

```rust
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
```
