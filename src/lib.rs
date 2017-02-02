extern crate futures;
extern crate tokio_core;

pub mod cached;

mod loader;
pub use loader::*;

#[cfg(test)]
mod tests {
    use super::{Loader, BatchFn, BatchFuture};

    use std::sync::atomic::{AtomicUsize, Ordering};
    use futures::Future;
    use futures::future::ok;

    pub struct Batcher {
        invoke_cnt: AtomicUsize,
        max_batch_size: usize,
    }

    impl Batcher {
        pub fn new(max_batch_size: usize) -> Batcher {
            Batcher {
                invoke_cnt: AtomicUsize::new(0),
                max_batch_size: max_batch_size,
            }
        }
    }

    impl BatchFn<i32, i32> for Batcher {
        type Error = ();
        fn load(&self, keys: &[i32]) -> BatchFuture<i32, Self::Error> {
            self.invoke_cnt.fetch_add(1, Ordering::SeqCst);
            ok(keys.into_iter().map(|v| v * 10).collect()).boxed()
        }

        fn max_batch_size(&self) -> usize {
            self.max_batch_size
        }
    }

    #[test]
    fn smoke() {
        let loader = Loader::new(Batcher::new(2)).cached();
        let v1 = loader.load(1);
        let v2 = loader.load(2);
        loader.clear(&2);
        let v3 = loader.load(3);
        assert_eq!((10, 20), v1.join(v2).wait().unwrap());
        assert_eq!(30, v3.wait().unwrap());

        let many = loader.load_many(vec![10, 20, 30]);
        assert_eq!(vec![100, 200, 300], many.wait().unwrap());

        loader.clear_all();
        loader.prime(2, 20);
        let loader_ref = &loader;
        {
            let v1 = loader_ref.load(1);
            let v2 = loader_ref.load(2);
            assert_eq!((10, 20), v1.join(v2).wait().unwrap());
        }
        {
            let v1 = loader_ref.load(3).map(|v| loader_ref.load(v).wait().unwrap());
            let v2 = loader_ref.load(4).map(|v| loader_ref.load(v).wait().unwrap());
            assert_eq!((300, 400), v1.join(v2).wait().unwrap());
        }
    }
}
