use std::collections::HashMap;

pub trait BatchFn<K, V> {
    fn load(&mut self, keys: &[K]) -> impl std::future::Future<Output = HashMap<K, V>>;
}
