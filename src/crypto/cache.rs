use crate::crypto::types::CryptoMarket;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

const CACHE_TTL: Duration = Duration::from_secs(300); // 5 minutes

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
            if cached.fetched_at.elapsed() < CACHE_TTL {
                return Some(cached.data.clone());
            }
            drop(cached);
            self.memory.remove(key);
        }
        None
    }

    pub fn set(&self, key: &str, data: Vec<CryptoMarket>) {
        self.memory.insert(
            key.to_string(),
            CachedMarkets {
                data,
                fetched_at: Instant::now(),
            },
        );
    }

    pub fn clear(&self) {
        self.memory.clear();
    }
}
