use async_trait::async_trait;
use std::collections::HashMap;

#[async_trait]
pub trait BatchFn<K, V> {
    async fn load(&self, keys: &[K]) -> HashMap<K, V>
    where
        K: 'async_trait,
        V: 'async_trait;
}
