use crate::crypto::types::CryptoMarket;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Instant;

pub struct CryptoMarketsCache {
    memory: Arc<DashMap<String, CachedMarkets>>,
}

struct CachedMarkets {
    data: Vec<CryptoMarket>,
    fetched_at: Instant,
}

impl CryptoMarketsCache {
    pub fn new() -> Self {
        Self {
            memory: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, key: &str) -> Option<Vec<CryptoMarket>> {
        if let Some(cached) = self.memory.get(key) {
            return Some(cached.data.clone());
        }
        None
    }

    pub fn set(&self, key: &str, data: Vec<CryptoMarket>) {
        self.memory.insert(
            key.to_string(),
            CachedMarkets {
                data: data.clone(),
                fetched_at: Instant::now(),
            },
        );
    }
}