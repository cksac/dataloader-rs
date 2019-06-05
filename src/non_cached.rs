use std::{collections::BTreeMap, mem, pin::Pin, rc::Rc, sync::Arc, thread};

use futures::{
    channel::{mpsc, oneshot},
    future, ready,
    task::Context,
    Future, FutureExt as _, Poll, Stream, StreamExt as _, TryFutureExt as _,
};
use tokio::runtime::current_thread;

use super::{cached, BatchFn, LoadError};

#[derive(Clone)]
pub struct Loader<K, V, E> {
    tx: Arc<mpsc::UnboundedSender<LoadMessage<K, V, E>>>,
}

impl<K, V, E> Loader<K, V, E> {
    pub fn load(&self, key: K) -> LoadFuture<V, E> {
        let (tx, rx) = oneshot::channel();
        let msg = Message::LoadOne { key, reply: tx };
        let _ = self.tx.unbounded_send(msg);
        LoadFuture { rx }
    }

    pub fn load_many(&self, keys: Vec<K>) -> future::TryJoinAll<LoadFuture<V, E>> {
        future::try_join_all(keys.into_iter().map(|v| self.load(v)))
    }

    pub fn cached(self) -> cached::Loader<K, V, E, BTreeMap<K, cached::LoadFuture<V, E>>>
    where
        K: Clone + Ord,
        V: Clone,
        E: Clone,
    {
        cached::Loader::new(self)
    }

    pub fn with_cache<C>(self, cache: C) -> cached::Loader<K, V, E, C>
    where
        K: Clone + Ord,
        V: Clone,
        E: Clone,
        C: cached::Cache<K, cached::LoadFuture<V, E>>,
    {
        cached::Loader::with_cache(self, cache)
    }
}

impl<K, V, E> Loader<K, V, E>
where
    K: 'static + Send + Unpin,
    V: 'static + Send,
    E: 'static + Clone + Send,
{
    pub fn new<F>(batch_fn: F) -> Loader<K, V, E>
    where
        F: 'static + Send + BatchFn<K, V, Error = E>,
    {
        assert!(batch_fn.max_batch_size() > 0);

        let (tx, rx) = mpsc::unbounded();
        let loader = Loader { tx: Arc::new(tx) };

        thread::spawn(move || {
            let batch_fn = Rc::new(batch_fn);
            let mut rt = current_thread::Runtime::new().unwrap();
            //let handle = rt.handle();

            let batched = Batched {
                rx,
                max_batch_size: batch_fn.max_batch_size(),
                items: Vec::with_capacity(batch_fn.max_batch_size()),
                channel_closed: false,
            };

            //let load_fn = batch_fn.clone();
            let loader = batched.for_each(move |requests| {
                let (keys, replys): (Vec<_>, Vec<_>) = requests.into_iter().unzip();
                batch_fn.load(&keys).map(move |x| match x {
                    Ok(values) => {
                        if keys.len() != values.len() {
                            let err = LoadError::UnequalKeyValueSize {
                                key_count: keys.len(),
                                value_count: values.len(),
                            };
                            for r in replys {
                                let _ = r.send(Err(err.clone()));
                            }
                        } else {
                            for r in replys.into_iter().zip(values) {
                                let _ = r.0.send(Ok(r.1));
                            }
                        }
                    }
                    Err(e) => {
                        let err = LoadError::BatchFn(e);
                        for r in replys {
                            let _ = r.send(Err(err.clone()));
                        }
                    }
                })
            });

            // Run until all batch jobs completed
            let _ = rt.spawn(loader.unit_error().boxed_local().compat()).run();
        });

        loader
    }
}

pub struct LoadFuture<V, E> {
    rx: oneshot::Receiver<Result<V, LoadError<E>>>,
}

impl<V, E> Future for LoadFuture<V, E> {
    type Output = Result<V, LoadError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        Poll::Ready(match ready!(self.rx.poll_unpin(cx)) {
            Ok(r) => r,
            Err(_) => Err(LoadError::SenderDropped),
        })
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

impl<K: Unpin, V, E> Stream for Batched<K, V, E> {
    type Item = Vec<LoadRequest<K, V, E>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.channel_closed {
            return Poll::Ready(None);
        }
        loop {
            match self.rx.poll_next_unpin(cx) {
                Poll::Pending => {
                    return if self.items.is_empty() {
                        Poll::Pending
                    } else {
                        let batch = mem::replace(&mut self.items, Vec::new());
                        Poll::Ready(Some(batch))
                    }
                }
                Poll::Ready(None) => {
                    self.channel_closed = true;
                    return if self.items.is_empty() {
                        Poll::Ready(None)
                    } else {
                        let batch = mem::replace(&mut self.items, Vec::new());
                        Poll::Ready(Some(batch))
                    };
                }
                Poll::Ready(Some(Message::LoadOne { key, reply })) => {
                    self.items.push((key, reply));
                    if self.items.len() >= self.max_batch_size {
                        let buf = Vec::with_capacity(self.max_batch_size);
                        let batch = mem::replace(&mut self.items, buf);
                        return Poll::Ready(Some(batch));
                    }
                }
            }
        }
    }
}
