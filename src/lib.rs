extern crate futures;
extern crate tokio_core;

use std::mem;
use std::thread;
use std::sync::Arc;

use futures::{Stream, Future, Poll, Async};
use futures::sync::{mpsc, oneshot};
use futures::future::{join_all, JoinAll};
use tokio_core::reactor::Core;

#[derive(Clone, Debug)]
pub enum LoadError {
    SenderDropped,
    Custom(String),
}

impl LoadError {
    pub fn custom<S: Into<String>>(s: S) -> LoadError {
        LoadError::Custom(s.into())
    }
}

pub trait BatchFn<K, V> {
    type Error: Into<LoadError>;
    fn load(&self, keys: &Vec<K>) -> Box<Future<Item = Vec<V>, Error = Self::Error>>;

    fn max_batch_size(&self) -> usize {
        200
    }
}

#[derive(Clone)]
pub struct Loader<K, V> {
    tx: Arc<mpsc::UnboundedSender<Message<K, Result<V, LoadError>>>>,
}

impl<K, V> Loader<K, V> {
    pub fn load(&self, key: K) -> LoadFuture<V> {
        let (tx, rx) = oneshot::channel();
        let msg = Message::LoadOne {
            key: key,
            reply: tx,
        };
        let _ = self.tx.send(msg);
        LoadFuture { rx: rx }
    }

    pub fn load_many(&self, keys: Vec<K>) -> JoinAll<Vec<LoadFuture<V>>> {
        join_all(keys.into_iter().map(|v| self.load(v)).collect())
    }
}

impl<K, V> Loader<K, V>
    where K: 'static + Send,
          V: 'static + Send
{
    pub fn new<F>(batch_fn: F) -> Loader<K, V>
        where F: 'static + Send + BatchFn<K, V>
    {
        assert!(batch_fn.max_batch_size() > 0);

        let (tx, rx) = mpsc::unbounded();
        let inner_handle = Arc::new(tx);
        let loader = Loader { tx: inner_handle };

        thread::spawn(move || {
            let batch_fn = Arc::new(batch_fn);
            let mut core = Core::new().unwrap();
            let handle = core.handle();

            let batched = Batched {
                rx: rx,
                max_batch_size: batch_fn.max_batch_size(),
                items: Vec::with_capacity(batch_fn.max_batch_size()),
                channel_closed: false,
            };

            let load_fn = batch_fn.clone();
            let loader =
                batched.for_each(move |requests: Vec<(K,
                                                      oneshot::Sender<Result<V, LoadError>>)>| {
                    let (keys, replys) = requests.into_iter()
                        .fold((Vec::new(), Vec::new()), |mut soa, i| {
                            soa.0.push(i.0);
                            soa.1.push(i.1);
                            soa
                        });
                    let batch_job = load_fn.load(&keys).then(move |x| {
                        match x {
                            Ok(values) => {
                                for r in replys.into_iter().zip(values) {
                                    r.0.complete(Ok(r.1));
                                }
                            }
                            Err(e) => {
                                let err = e.into();
                                for r in replys {
                                    r.complete(Err(err.clone()));
                                }
                            }
                        };
                        Ok(())
                    });
                    handle.spawn(batch_job);
                    Ok(())
                });
            let _ = core.run(loader);

            // Run until all batch jobs completed
            core.turn(None);
        });

        loader
    }
}

pub struct LoadFuture<V> {
    rx: oneshot::Receiver<Result<V, LoadError>>,
}

impl<V> Future for LoadFuture<V> {
    type Item = V;
    type Error = LoadError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(Ok(v))) => Ok(Async::Ready(v)),
            Ok(Async::Ready(Err(e))) => Err(e),
            Err(_) => Err(LoadError::SenderDropped),
        }
    }
}

enum Message<K, V> {
    LoadOne { key: K, reply: oneshot::Sender<V> },
}

struct Batched<K, V> {
    rx: mpsc::UnboundedReceiver<Message<K, Result<V, LoadError>>>,
    max_batch_size: usize,
    items: Vec<(K, oneshot::Sender<Result<V, LoadError>>)>,
    channel_closed: bool,
}

impl<K, V> Stream for Batched<K, V> {
    type Item = Vec<(K, oneshot::Sender<Result<V, LoadError>>)>;
    type Error = LoadError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.channel_closed {
            return Ok(Async::Ready(None));
        }
        loop {
            match self.rx.poll() {
                Ok(Async::NotReady) => {
                    return if self.items.len() > 0 {
                        let batch = mem::replace(&mut self.items, Vec::new());
                        Ok(Some(batch).into())
                    } else {
                        Ok(Async::NotReady)
                    };
                }
                Ok(Async::Ready(Some(msg))) => {
                    match msg {
                        Message::LoadOne { key, reply } => {
                            self.items.push((key, reply));
                            if self.items.len() >= self.max_batch_size {
                                let buf = Vec::with_capacity(self.max_batch_size);
                                let batch = mem::replace(&mut self.items, buf);
                                return Ok(Some(batch).into());
                            }
                        }
                    }
                }
                Ok(Async::Ready(None)) => {
                    return if self.items.len() > 0 {
                        let batch = mem::replace(&mut self.items, Vec::new());
                        Ok(Some(batch).into())
                    } else {
                        Ok(Async::Ready(None))
                    };
                }
                Err(_) => {
                    return if self.items.len() == 0 {
                        Ok(Async::Ready(None))
                    } else {
                        self.channel_closed = true;
                        let batch = mem::replace(&mut self.items, Vec::new());
                        Ok(Some(batch).into())
                    };
                }
            }
        }
    }
}
