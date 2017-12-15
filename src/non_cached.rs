use {BatchFn, LoadError};
use cached;

use std::mem;
use std::thread;
use std::sync::Arc;
use std::collections::BTreeMap;

use futures::{Stream, Future, Poll, Async};
use futures::sync::{mpsc, oneshot};
use futures::future::{join_all, JoinAll};
use tokio_core::reactor::Core;


#[derive(Clone)]
pub struct Loader<K, V, E> {
    tx: Arc<mpsc::UnboundedSender<LoadMessage<K, V, E>>>,
}

impl<K, V, E> Loader<K, V, E> {
    pub fn load(&self, key: K) -> LoadFuture<V, E> {
        let (tx, rx) = oneshot::channel();
        let msg = Message::LoadOne {
            key: key,
            reply: tx,
        };
        let _ = self.tx.unbounded_send(msg);
        LoadFuture { rx: rx }
    }

    pub fn load_many(&self, keys: Vec<K>) -> JoinAll<Vec<LoadFuture<V, E>>> {
        join_all(keys.into_iter().map(|v| self.load(v)).collect())
    }

    pub fn cached(self) -> cached::Loader<K, V, E, BTreeMap<K, cached::LoadFuture<V, E>>>
        where K: Clone + Ord,
              V: Clone,
              E: Clone
    {
        cached::Loader::new(self)
    }

    pub fn with_cache<C>(self, cache: C) -> cached::Loader<K, V, E, C>
        where K: Clone + Ord,
              V: Clone,
              E: Clone,
              C: cached::Cache<K, cached::LoadFuture<V, E>>
    {
        cached::Loader::with_cache(self, cache)
    }
}

impl<K, V, E> Loader<K, V, E>
    where K: 'static + Send,
          V: 'static + Send,
          E: 'static + Clone + Send
{
    pub fn new<F>(batch_fn: F) -> Loader<K, V, E>
        where F: 'static + Send + BatchFn<K, V, Error = E>
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
            let loader = batched.for_each(move |requests: Vec<LoadRequest<K, V, E>>| {
                let (keys, replys) = requests.into_iter()
                    .fold((Vec::new(), Vec::new()), |mut soa, i| {
                        soa.0.push(i.0);
                        soa.1.push(i.1);
                        soa
                    });
                let batch_job = load_fn.load(&keys).then(move |x| {
                    match x {
                        Ok(values) => {
                            if keys.len() != values.len() {
                                for r in replys {
                                    let err = LoadError::UnequalKeyValueSize {
                                        key_count: keys.len(),
                                        value_count: values.len(),
                                    };
                                   let _ =  r.send(Err(err.clone()));
                                }
                            } else {
                                for r in replys.into_iter().zip(values) {
                                   let _ =  r.0.send(Ok(r.1));
                                }
                            }
                        }
                        Err(e) => {
                            let err = LoadError::BatchFn(e);
                            for r in replys {
                                let _ = r.send(Err(err.clone()));
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

pub struct LoadFuture<V, E> {
    rx: oneshot::Receiver<Result<V, LoadError<E>>>,
}

impl<V, E> Future for LoadFuture<V, E> {
    type Item = V;
    type Error = LoadError<E>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(Ok(v))) => Ok(Async::Ready(v)),
            Ok(Async::Ready(Err(e))) => Err(e),
            Err(_) => Err(LoadError::SenderDropped),
        }
    }
}

type LoadRequest<K, V, E> = (K, oneshot::Sender<Result<V, LoadError<E>>>);
type LoadMessage<K, V, E> = Message<K, Result<V, LoadError<E>>>;

enum Message<K, V> {
    LoadOne { key: K, reply: oneshot::Sender<V> },
}

struct Batched<K, V, E> {
    rx: mpsc::UnboundedReceiver<LoadMessage<K, V, E>>,
    max_batch_size: usize,
    items: Vec<LoadRequest<K, V, E>>,
    channel_closed: bool,
}

impl<K, V, E> Stream for Batched<K, V, E> {
    type Item = Vec<LoadRequest<K, V, E>>;
    type Error = LoadError<E>;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.channel_closed {
            return Ok(Async::Ready(None));
        }
        loop {
            match self.rx.poll() {
                Ok(Async::NotReady) => {
                    return if self.items.is_empty() {
                        Ok(Async::NotReady)
                    } else {
                        let batch = mem::replace(&mut self.items, Vec::new());
                        Ok(Some(batch).into())
                    }
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
                    return if self.items.is_empty() {
                        Ok(Async::Ready(None))
                    } else {
                        let batch = mem::replace(&mut self.items, Vec::new());
                        Ok(Some(batch).into())
                    }
                }
                Err(_) => {
                    return if self.items.is_empty() {
                        Ok(Async::Ready(None))
                    } else {
                        self.channel_closed = true;
                        let batch = mem::replace(&mut self.items, Vec::new());
                        Ok(Some(batch).into())
                    }
                }
            }
        }
    }
}
