use std::{
    cell::RefCell,
    collections::BTreeMap,
    mem,
    pin::Pin,
    sync::{Arc, Mutex},
};

use futures::{
    channel::{mpsc, oneshot},
    future, ready,
    task::{Context, Waker},
    Future, FutureExt as _, Poll, Stream, StreamExt as _,
};

use super::{cached, BatchFn, BatchFuture, LoadError};

//#[derive(Clone)]
pub struct Loader<K, V, E> {
    //todos: Arc<Mutex<BTreeMap<K, Option<Result<V, LoadError<E>>>>>>,
    queue: Arc<Mutex<Vec<Option<(K, Option<Waker>, Option<Result<V, LoadError<E>>>)>>>>,
    batch_fn: Arc<BatchFn<K, V, Error = E>>,
    current_load: Arc<Mutex<Option<BatchFuture<V, E>>>>,
    visited_count: Arc<Mutex<usize>>,
    max_batch_size: usize,
    //tx: Arc<mpsc::UnboundedSender<LoadMessage<K, V, E>>>,
}

impl<K, V, E> Clone for Loader<K, V, E> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            batch_fn: self.batch_fn.clone(),
            current_load: self.current_load.clone(),
            visited_count: self.visited_count.clone(),
            max_batch_size: self.max_batch_size,
        }
    }
}

impl<K, V, E> Loader<K, V, E> {
    pub fn load(&self, key: K) -> LoadFuture2<K, V, E>
    //pub fn load(&self, key: K) -> impl Future<Output = Result<V, LoadError<E>>>
    where
        K: Clone,
        E: Clone,
    {
        let index: usize;
        {
            let mut queue = self.queue.lock().unwrap();
            queue.push(Some((key, None, None)));
            index = queue.len() - 1;
        }

        /*
        {
            let mut todos = self.todos.lock().unwrap();

            todos.insert(dbg!(key.clone()), None);
            // TODO: check if exists and not None, return immediately
        }
        let todos = self.todos.clone();
        let batch_fn = self.batch_fn.clone();
        future::lazy(move |_| {
            let loc_todos = todos.clone();
            let mut loc_todos = loc_todos.lock().unwrap();
            if let Some(None) = loc_todos.get(dbg!(&key)) {
                let keys: Vec<_> = loc_todos
                    .iter()
                    .filter_map(|(k, v)| if v.is_some() { None } else { Some(k.clone()) })
                    .collect();
                batch_fn
                    .load(&keys)
                    .map(move |res| match res {
                        Ok(vals) => {
                            if keys.len() != vals.len() {
                                let err = LoadError::UnequalKeyValueSize {
                                    key_count: keys.len(),
                                    value_count: vals.len(),
                                };
                                for (_, val) in todos.lock().unwrap().iter_mut() {
                                    *val = Some(Err(err.clone()))
                                }
                            } else {
                                let mut todos = todos.lock().unwrap();
                                for (key, val) in keys.into_iter().zip(vals.into_iter()) {
                                    //dbg!((&key, &val));
                                    let _ = todos.insert(key, Some(Ok(val)));
                                }
                            }
                            todos
                        }
                        Err(err) => {
                            for (_, val) in todos.lock().unwrap().iter_mut() {
                                *val = Some(Err(LoadError::BatchFn(err.clone())))
                            }
                            todos
                        }
                    })
                    .map(move |todos| todos.lock().unwrap().remove(&key).unwrap().unwrap())
                    .right_future()
            } else {
                future::ready(loc_todos.remove(&key).unwrap().unwrap()).left_future()
            }
        })
        .flatten()
        */
        /* TODO
        if self.items.len() >= self.max_batch_size {
            let buf = Vec::with_capacity(self.max_batch_size);
            let batch = mem::replace(&mut self.items, buf);
            return Poll::Ready(Some(batch));
        }*/

        //let (tx, rx) = oneshot::channel();
        //let msg = Message::LoadOne { key, reply: tx };
        //let _ = self.tx.unbounded_send(msg);
        //LoadFuture { rx }
        LoadFuture2 {
            index,
            visited: RefCell::new(false),
            loader: self.clone(),
        }
    }

    //pub fn load_many(&self, keys: Vec<K>) -> future::TryJoinAll<LoadFuture<V, E>> {
    pub fn load_many(&self, keys: Vec<K>) -> impl Future<Output = Result<Vec<V>, LoadError<E>>>
    where
        K: Clone + std::fmt::Debug,
        E: Clone,
    {
        future::try_join_all(keys.into_iter().map(|v| self.load(v)))
    }

    pub fn cached(self) -> cached::Loader<K, V, E, BTreeMap<K, Result<V, LoadError<E>>>>
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
        C: cached::Cache<K, Result<V, LoadError<E>>>,
    {
        cached::Loader::with_cache(self, cache)
    }
}

impl<K, V, E> Loader<K, V, E>
where
    K: 'static + Send + Unpin + Clone + Ord,
    V: 'static + Send,
    E: 'static + Clone + Send,
{
    pub fn new<F>(batch_fn: F) -> Loader<K, V, E>
    where
        F: 'static + Send + BatchFn<K, V, Error = E>,
    {
        assert!(batch_fn.max_batch_size() > 0);

        //let (tx, rx) = mpsc::unbounded();
        let max_batch_size = batch_fn.max_batch_size();
        let loader = Loader {
            //todos: Arc::new(Mutex::new(BTreeMap::new())),
            queue: Arc::new(Mutex::new(vec![])),
            batch_fn: Arc::new(batch_fn),
            current_load: Arc::new(Mutex::new(None)),
            visited_count: Arc::new(Mutex::new(0)),
            max_batch_size,
            //tx: Arc::new(tx),
        };

        /*
        let batch_fn = loader.batch_fn.clone();
        thread::spawn(move || {
            //let batch_fn = Rc::new(batch_fn);
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
        });*/

        loader
    }
}

pub struct LoadFuture2<K, V, E> {
    index: usize,
    visited: RefCell<bool>,
    loader: Loader<K, V, E>,
}

impl<K: Clone, V, E: Clone> Future
    for LoadFuture2<K, V, E>
{
    type Output = Result<V, LoadError<E>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        //     1. If visited_count == queue.len() then run batch_function to completion and return resolved result (with removing from queue)
        //     2. Else increment visited_count, save Waker and return Pending if visited_count != queue.len()
        //     3. But if visited_count count == queue.len() then wake all futures and return Pending

        let mut visited_count = self.loader.visited_count.lock().unwrap();
        let mut queue = self.loader.queue.lock().unwrap();

        if queue[self.index].is_some() && queue[self.index].as_ref().unwrap().2.is_some() {
            let item = queue[self.index].take().unwrap();
            //dbg!(&queue);
            return Poll::Ready(item.2.unwrap());
        }

        //dbg!(self.index, queue[self.index].as_ref().map(|t| &t.0));

        if *visited_count == queue.len() {
            let mut current_load = self.loader.current_load.lock().unwrap();

            // TODO: optimize
            let (indexes, keys): (Vec<_>, Vec<_>) = queue
                .iter()
                .enumerate()
                .filter_map(|(n, item)| {
                    if item.is_some() {
                        return Some((n, item.as_ref().unwrap().0.clone()));
                    }
                    None
                })
                .unzip();
            //dbg!(&keys);

            // TODO: what if multiple loads requested?
            if current_load.is_none() {
                *current_load = Some(self.loader.batch_fn.load(&keys));
            }

            match ready!(Pin::new(current_load.as_mut().unwrap()).poll(cx)) {
                Ok(vals) => {
                    //dbg!(&vals);
                    if keys.len() != vals.len() {
                        let err = LoadError::UnequalKeyValueSize {
                            key_count: keys.len(),
                            value_count: vals.len(),
                        };
                        for index in indexes {
                            if let Some(ref mut item) = queue[index] {
                                item.2 = Some(Err(err.clone()));
                            }
                        }
                    } else {
                        for (index, val) in indexes.into_iter().zip(vals.into_iter()) {
                            if let Some(ref mut item) = queue[index] {
                                item.2 = Some(Ok(val));
                            }
                        }
                    }
                }
                Err(err) => {
                    //for (i, (index, _)) in pairs.iter().enumerate() {
                    for index in indexes {
                        if let Some(ref mut item) = queue[index] {
                            item.2 = Some(Err(LoadError::BatchFn(err.clone())));
                        }
                    }
                }
            }
            *current_load = None;
            //dbg!(&queue);

            let item = queue[self.index].take().unwrap();
            //dbg!(&queue);
            return Poll::Ready(item.2.unwrap());
        }

        if !*self.visited.borrow() {
            *visited_count += 1;
            *self.as_ref().visited.borrow_mut() = true;
            //dbg!(self.index, &*visited_count);
        }
        queue[self.index].as_mut().unwrap().1 = Some(cx.waker().clone());
        if *visited_count == queue.len() {
            // When last loading future is polled, we reschedule all the loading
            // futures again to the end of event loop queue.
            // This is required for better batch loading deferring, as allows
            // more event loop ticks before doing a batch load, in which new
            // keys may be added.
            for (_, ref mut waker, _) in queue.iter_mut().filter_map(|i| i.as_mut()) {
                if let Some(waker) = waker.take() {
                    waker.wake();
                }
            }
            //dbg!("run wakers");
        }
        Poll::Pending
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
