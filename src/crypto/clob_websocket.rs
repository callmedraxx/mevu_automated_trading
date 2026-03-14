//! CLOB WebSocket client for real-time Polymarket price updates.
//! Connects to wss://ws-subscriptions-clob.polymarket.com/ws/market
//! Subscribes to BTC market tokens, receives price_change events,
//! updates in-memory price cache, and updates PostgreSQL immediately.

use crate::crypto::types::{ClobPriceChangeEvent, LivePrice};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_tungstenite::tungstenite::Message;

const CLOB_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const PING_INTERVAL: Duration = Duration::from_secs(10);
const MAX_TOKENS_PER_SHARD: usize = 200;
const RECONNECT_BASE_DELAY: Duration = Duration::from_secs(5);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(300);

/// Maps clob_token_id → (slug, outcome "up"/"down")
pub type TokenMap = Arc<DashMap<String, (String, String)>>;

/// Maps clob_token_id → LivePrice (latest real-time price)
pub type PriceCache = Arc<DashMap<String, LivePrice>>;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Start the CLOB WebSocket system.
/// Spawns connection manager. Updates DB immediately and broadcasts to frontend WS clients.
pub fn start_clob_websocket(
    token_map: TokenMap,
    price_cache: PriceCache,
    db: Option<sqlx::PgPool>,
    price_updates_tx: tokio::sync::broadcast::Sender<String>,
) {
    tokio::spawn(async move {
        connection_manager(token_map, price_cache, db, price_updates_tx).await;
    });
}

/// Rebuilds the token map from current BTC markets.
/// Called after each market refresh.
pub fn rebuild_token_map(
    token_map: &TokenMap,
    markets: &[crate::crypto::types::CryptoMarket],
) {
    token_map.clear();
    for market in markets {
        if let Some(up_token) = &market.up_clob_token_id {
            if !up_token.is_empty() {
                token_map.insert(up_token.clone(), (market.slug.clone(), "up".into()));
            }
        }
        if let Some(down_token) = &market.down_clob_token_id {
            if !down_token.is_empty() {
                token_map.insert(down_token.clone(), (market.slug.clone(), "down".into()));
            }
        }
    }
    tracing::info!("Token map rebuilt: {} tokens tracked", token_map.len());
}

/// Manages WebSocket connection lifecycle with reconnection logic.
async fn connection_manager(
    token_map: TokenMap,
    price_cache: PriceCache,
    db: Option<sqlx::PgPool>,
    price_updates_tx: tokio::sync::broadcast::Sender<String>,
) {
    let mut attempt = 0u32;

    loop {
        let token_ids: Vec<String> = token_map.iter().map(|e| e.key().clone()).collect();

        if token_ids.is_empty() {
            tracing::info!("No tokens to subscribe to yet, waiting for market refresh...");
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue;
        }

        let shards: Vec<Vec<String>> = token_ids
            .chunks(MAX_TOKENS_PER_SHARD)
            .map(|c| c.to_vec())
            .collect();

        tracing::info!(
            "Connecting to CLOB WebSocket: {} tokens across {} shard(s)",
            token_ids.len(),
            shards.len()
        );

        let mut handles = Vec::new();
        for (i, shard) in shards.into_iter().enumerate() {
            let tm = token_map.clone();
            let pc = price_cache.clone();
            let db2 = db.clone();
            let tx = price_updates_tx.clone();
            handles.push(tokio::spawn(async move {
                run_shard(i, shard, tm, pc, db2, tx).await
            }));
        }

        let _ = futures_util::future::select_all(handles).await;

        attempt += 1;
        let delay = std::cmp::min(
            RECONNECT_BASE_DELAY * 2u32.saturating_pow(attempt.min(6)),
            MAX_RECONNECT_DELAY,
        );
        tracing::warn!(
            "CLOB WebSocket disconnected, reconnecting in {:?} (attempt {})",
            delay,
            attempt
        );
        tokio::time::sleep(delay).await;
    }
}

/// Run a single shard WebSocket connection.
async fn run_shard(
    shard_idx: usize,
    token_ids: Vec<String>,
    token_map: TokenMap,
    price_cache: PriceCache,
    db: Option<sqlx::PgPool>,
    price_updates_tx: tokio::sync::broadcast::Sender<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (ws_stream, _) = tokio_tungstenite::connect_async(CLOB_WS_URL).await?;
    let (mut write, mut read) = ws_stream.split();

    tracing::info!(
        "CLOB shard {} connected, subscribing to {} tokens",
        shard_idx,
        token_ids.len()
    );

    let sub_msg = serde_json::json!({
        "assets_ids": token_ids,
        "type": "market"
    });
    write.send(Message::Text(sub_msg.to_string().into())).await?;

    let mut ping_write = write;
    let ping_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(PING_INTERVAL);
        loop {
            interval.tick().await;
            if ping_write.send(Message::Text("PING".into())).await.is_err() {
                break;
            }
        }
    });

    while let Some(msg_result) = read.next().await {
        let msg = match msg_result {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("CLOB shard {} read error: {}", shard_idx, e);
                break;
            }
        };

        match msg {
            Message::Text(text) => {
                let text_str: &str = &text;
                if text_str == "PONG" {
                    continue;
                }
                if let Ok(event) = serde_json::from_str::<ClobPriceChangeEvent>(text_str) {
                    if event.event_type == "price_change" {
                        process_price_changes(
                            &event,
                            &token_map,
                            &price_cache,
                            &db,
                            &price_updates_tx,
                        )
                        .await;
                    }
                }
            }
            Message::Close(_) => {
                tracing::info!("CLOB shard {} received close frame", shard_idx);
                break;
            }
            _ => {}
        }
    }

    ping_handle.abort();
    Ok(())
}

/// Process price_change events: update in-memory cache, DB, and broadcast to WS clients.
async fn process_price_changes(
    event: &ClobPriceChangeEvent,
    token_map: &TokenMap,
    price_cache: &PriceCache,
    db: &Option<sqlx::PgPool>,
    price_updates_tx: &tokio::sync::broadcast::Sender<String>,
) {
    let changes = match &event.price_changes {
        Some(c) => c,
        None => return,
    };

    let timestamp = event
        .timestamp
        .as_ref()
        .and_then(|t| t.parse::<u64>().ok())
        .unwrap_or_else(now_ms);

    for change in changes {
        let token_id = match &change.asset_id {
            Some(id) => id,
            None => continue,
        };

        let (slug, outcome) = match token_map.get(token_id) {
            Some(entry) => entry.value().clone(),
            None => continue,
        };

        let best_bid = change
            .best_bid
            .as_ref()
            .and_then(|b| b.parse::<f64>().ok())
            .unwrap_or(0.0);

        let best_ask = match &change.best_ask {
            Some(a) => match a.parse::<f64>() {
                Ok(v) if v > 0.0 && v < 1.0 => v,
                _ => continue,
            },
            None => continue,
        };

        let price_cents = (best_ask * 100.0).round() as u32;

        // Skip if price hasn't changed (deduplication)
        let changed = match price_cache.get(token_id) {
            Some(existing) => existing.price_cents != price_cents,
            None => true,
        };

        if !changed {
            continue;
        }

        let live_price = LivePrice {
            clob_token_id: token_id.clone(),
            slug: slug.clone(),
            outcome: outcome.clone(),
            best_bid,
            best_ask,
            price_cents,
            updated_at_ms: timestamp,
        };

        // Update in-memory cache immediately
        price_cache.insert(token_id.clone(), live_price.clone());

        // Broadcast price update to all connected frontend WebSocket clients
        let broadcast_msg = serde_json::json!({
            "type": "price_update",
            "slug": slug,
            "outcome": outcome,
            "best_ask": best_ask,
            "ts": timestamp,
        })
        .to_string();
        let _ = price_updates_tx.send(broadcast_msg); // ok if no receivers yet

        // Update DB immediately (no batch flush - we only track BTC so volume is low)
        if let Some(pool) = db {
            update_db_price(pool, &live_price).await;
        }
    }
}

/// Update price in PostgreSQL immediately for a single token.
async fn update_db_price(pool: &sqlx::PgPool, lp: &LivePrice) {
    let price_result = if lp.outcome == "up" {
        sqlx::query(
            "UPDATE crypto_markets SET up_price = $1, updated_at = NOW() WHERE slug = $2",
        )
        .bind(lp.best_ask)
        .bind(&lp.slug)
        .execute(pool)
        .await
    } else {
        sqlx::query(
            "UPDATE crypto_markets SET down_price = $1, updated_at = NOW() WHERE slug = $2",
        )
        .bind(lp.best_ask)
        .bind(&lp.slug)
        .execute(pool)
        .await
    };

    if let Err(e) = price_result {
        tracing::warn!("DB price update error for {}: {}", lp.slug, e);
        return;
    }

    // Update direction based on new prices
    if let Err(e) = sqlx::query(
        "UPDATE crypto_markets SET direction = CASE
            WHEN up_price > down_price THEN 'up'
            WHEN down_price > up_price THEN 'down'
            ELSE direction
        END
        WHERE slug = $1",
    )
    .bind(&lp.slug)
    .execute(pool)
    .await
    {
        tracing::warn!("DB direction update error for {}: {}", lp.slug, e);
    }
}
