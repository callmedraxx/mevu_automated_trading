//! CLOB WebSocket client for real-time Polymarket price updates.
//! Connects to wss://ws-subscriptions-clob.polymarket.com/ws/market
//! Subscribes to BTC market tokens, receives price_change events,
//! updates in-memory price cache, broadcasts instantly to frontend,
//! and batches DB writes every 2 seconds (deduped by token).
//!
//! Each shard manages its own reconnection independently — a single shard
//! disconnecting does NOT tear down the other shards.

use crate::crypto::types::{ClobPriceChangeEvent, LivePrice};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::Message;

const CLOB_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const PING_INTERVAL: Duration = Duration::from_secs(10);
/// If no PONG (or any message) received within this window, consider connection dead.
const PONG_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_TOKENS_PER_SHARD: usize = 50;
/// Delay between spawning each shard on startup (avoids simultaneous connections)
const SHARD_STAGGER_DELAY: Duration = Duration::from_millis(500);
/// Base reconnect delay for rapid failures
const BASE_RECONNECT_DELAY: Duration = Duration::from_secs(1);
/// Maximum reconnect delay (cap — never wait longer than this)
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(8);
const DB_FLUSH_INTERVAL: Duration = Duration::from_secs(2);

/// If a shard stays alive longer than this, the next disconnect is treated as
/// a fresh start (attempt counter resets).
const STABLE_CONNECTION_THRESHOLD: Duration = Duration::from_secs(60);

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
/// Returns nothing — state.clob_reconnect can be used to trigger immediate reconnect.
pub fn start_clob_websocket(
    token_map: TokenMap,
    price_cache: PriceCache,
    db: Option<sqlx::PgPool>,
    price_updates_tx: tokio::sync::broadcast::Sender<String>,
    reconnect: Arc<Notify>,
) {
    // Channel for batching DB writes: sender lives in process_price_changes,
    // receiver lives in db_flush_task. Unbounded so the WS loop never blocks.
    let (db_tx, db_rx) = tokio::sync::mpsc::unbounded_channel::<LivePrice>();

    // Spawn background DB flush task (2-second batch window)
    if let Some(pool) = db.clone() {
        tokio::spawn(db_flush_task(db_rx, pool));
    } else {
        // No DB — drain the channel to avoid memory build-up
        tokio::spawn(async move {
            let mut rx = db_rx;
            while rx.recv().await.is_some() {}
        });
    }

    tokio::spawn(async move {
        shard_manager(token_map, price_cache, db_tx, price_updates_tx, reconnect).await;
    });
}

/// Rebuilds the token map from current BTC markets and triggers reconnect.
/// Called after each market refresh.
///
/// Filters out:
/// - Markets whose end_date has passed (with 2-minute grace period)
/// - Markets ending after today (only subscribe to today's markets)
pub fn rebuild_token_map(
    token_map: &TokenMap,
    markets: &[crate::crypto::types::CryptoMarket],
) {
    use chrono::{Utc, NaiveDate};

    let now = Utc::now();
    // 2-minute grace: treat markets as still active for 2 mins past end_date
    let cutoff = now - chrono::Duration::seconds(120);
    // End of today (UTC): only subscribe to markets ending today or earlier
    let today = now.date_naive();
    let end_of_day = today
        .succ_opt()
        .unwrap_or(today)
        .and_hms_opt(0, 0, 0)
        .unwrap();

    token_map.clear();
    let mut included = 0u32;
    let mut skipped_past = 0u32;
    let mut skipped_future = 0u32;

    for market in markets {
        // Check end_date: skip markets that ended more than 2 minutes ago
        if let Some(ref end_str) = market.end_date {
            // Try parsing as RFC3339 timestamp (e.g. "2026-03-17T18:30:00Z")
            let parsed = chrono::DateTime::parse_from_rfc3339(end_str)
                .map(|dt| dt.with_timezone(&Utc))
                // Fallback: try ISO 8601 with space separator
                .or_else(|_| {
                    chrono::NaiveDateTime::parse_from_str(end_str, "%Y-%m-%dT%H:%M:%S")
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(end_str, "%Y-%m-%d %H:%M:%S"))
                        .map(|ndt| ndt.and_utc())
                });

            if let Ok(end_utc) = parsed {
                if end_utc < cutoff {
                    skipped_past += 1;
                    continue;
                }
                // Skip markets ending after today (future days)
                if end_utc.date_naive() > today {
                    skipped_future += 1;
                    continue;
                }
            } else if let Ok(end_naive) = NaiveDate::parse_from_str(end_str, "%Y-%m-%d") {
                if end_naive < today {
                    skipped_past += 1;
                    continue;
                }
                if end_naive > today {
                    skipped_future += 1;
                    continue;
                }
            }
        }

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
        included += 1;
    }

    tracing::info!(
        "Token map rebuilt: {} tokens from {} markets (skipped {} ended, {} future-day)",
        token_map.len(), included, skipped_past, skipped_future
    );
}

/// Manages shard lifecycle. Each shard runs independently with its own reconnect loop.
/// Only a forced reconnect (market refresh) tears down all shards to re-shard with new tokens.
async fn shard_manager(
    token_map: TokenMap,
    price_cache: PriceCache,
    db_tx: tokio::sync::mpsc::UnboundedSender<LivePrice>,
    price_updates_tx: tokio::sync::broadcast::Sender<String>,
    reconnect: Arc<Notify>,
) {
    loop {
        let token_ids: Vec<String> = token_map.iter().map(|e| e.key().clone()).collect();

        if token_ids.is_empty() {
            tracing::info!("No tokens to subscribe to yet, waiting for market refresh...");
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                _ = reconnect.notified() => {}
            }
            continue;
        }

        let shards: Vec<Vec<String>> = token_ids
            .chunks(MAX_TOKENS_PER_SHARD)
            .map(|c| c.to_vec())
            .collect();

        let num_shards = shards.len();
        tracing::info!(
            "Starting CLOB WebSocket: {} tokens across {} shard(s)",
            token_ids.len(),
            num_shards
        );

        // Each shard gets its own CancellationToken so we can kill them all on forced reconnect
        let cancel = tokio_util::sync::CancellationToken::new();

        // Spawn each shard as an independent self-healing task, staggered to avoid
        // opening all connections simultaneously (Polymarket rate limits)
        let mut handles = Vec::new();
        for (i, shard_tokens) in shards.into_iter().enumerate() {
            let tm = token_map.clone();
            let pc = price_cache.clone();
            let tx = price_updates_tx.clone();
            let db_tx2 = db_tx.clone();
            let shard_cancel = cancel.clone();
            let stagger = SHARD_STAGGER_DELAY * i as u32;
            handles.push(tokio::spawn(async move {
                // Stagger: shard 0 starts immediately, shard 1 after 500ms, etc.
                if !stagger.is_zero() {
                    tokio::time::sleep(stagger).await;
                }
                run_shard_with_reconnect(i, shard_tokens, tm, pc, db_tx2, tx, shard_cancel).await;
            }));
        }

        // Wait for forced reconnect signal (e.g. market refresh rebuilds token map)
        reconnect.notified().await;
        tracing::info!("CLOB forced reconnect: cancelling all {} shards and re-sharding", num_shards);

        // Cancel all shard tasks — they'll exit their reconnect loops
        cancel.cancel();

        // Wait for all shard tasks to finish cleanly
        for h in handles {
            let _ = h.await;
        }
    }
}

/// Run a single shard with its own independent reconnect loop.
/// Only exits when the cancellation token is triggered (forced reconnect).
///
/// Reconnect strategy:
///   - After a stable connection (>60s alive): near-immediate (1s + jitter)
///   - Rapid failures: 1s → 2s → 4s → 8s cap (never longer)
///   - Per-shard jitter (based on shard index) prevents thundering herd
async fn run_shard_with_reconnect(
    shard_idx: usize,
    token_ids: Vec<String>,
    token_map: TokenMap,
    price_cache: PriceCache,
    db_tx: tokio::sync::mpsc::UnboundedSender<LivePrice>,
    price_updates_tx: tokio::sync::broadcast::Sender<String>,
    cancel: tokio_util::sync::CancellationToken,
) {
    let mut attempt = 0u32;

    loop {
        // Check if we should stop (forced reconnect / re-shard)
        if cancel.is_cancelled() {
            tracing::debug!("CLOB shard {} shutting down (cancelled)", shard_idx);
            return;
        }

        let connect_time = tokio::time::Instant::now();

        // Run the shard connection
        let result = tokio::select! {
            r = run_shard(shard_idx, &token_ids, &token_map, &price_cache, &db_tx, &price_updates_tx) => r,
            _ = cancel.cancelled() => {
                tracing::debug!("CLOB shard {} cancelled during run", shard_idx);
                return;
            }
        };

        // Shard disconnected — only reconnect this shard
        if let Err(e) = &result {
            tracing::warn!("CLOB shard {} error: {}", shard_idx, e);
        }

        // If we were connected long enough, reset attempt counter
        if connect_time.elapsed() >= STABLE_CONNECTION_THRESHOLD {
            attempt = 0;
        }

        attempt += 1;

        // Exponential backoff: 2s, 4s, 8s, 16s, 30s cap
        let base_delay = if attempt <= 1 {
            BASE_RECONNECT_DELAY
        } else {
            let exp = BASE_RECONNECT_DELAY * 2u32.saturating_pow(attempt - 1);
            exp.min(MAX_RECONNECT_DELAY)
        };

        // Per-shard jitter: spread reconnects so shards don't all hit the server at once
        // shard 0 adds 0-500ms, shard 1 adds 500-1000ms, etc.
        let jitter_base_ms = (shard_idx as u64) * 500;
        let jitter_random_ms = (now_ms() % 500) + jitter_base_ms;
        let delay = base_delay + Duration::from_millis(jitter_random_ms);

        tracing::warn!(
            "CLOB shard {} disconnected, reconnecting in {:.1}s (attempt {})",
            shard_idx, delay.as_secs_f64(), attempt
        );
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = cancel.cancelled() => {
                tracing::debug!("CLOB shard {} cancelled during reconnect delay", shard_idx);
                return;
            }
        }
    }
}

/// Run a single shard WebSocket connection. Returns when the connection drops.
///
/// Sends both application-level "PING" text messages (Polymarket protocol) and
/// WebSocket-level Ping frames every PING_INTERVAL. If no message (including PONG)
/// is received within PONG_TIMEOUT, the connection is considered dead and we bail
/// so the shard can reconnect.
async fn run_shard(
    shard_idx: usize,
    token_ids: &[String],
    token_map: &TokenMap,
    price_cache: &PriceCache,
    db_tx: &tokio::sync::mpsc::UnboundedSender<LivePrice>,
    price_updates_tx: &tokio::sync::broadcast::Sender<String>,
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

    // Ping sender: sends both application-level "PING" and WebSocket Ping frames
    let (ping_tx, mut ping_rx) = tokio::sync::mpsc::channel::<()>(1);
    let ping_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(PING_INTERVAL);
        loop {
            interval.tick().await;
            // Application-level PING (Polymarket protocol)
            if write.send(Message::Text("PING".into())).await.is_err() {
                break;
            }
            // WebSocket-level Ping frame (protocol keepalive)
            if write.send(Message::Ping(vec![].into())).await.is_err() {
                break;
            }
            // Signal that pings were sent (so reader can track timing)
            let _ = ping_tx.try_send(());
        }
    });

    // Track last time we received ANY message (data, PONG, or Pong frame)
    let mut last_activity = tokio::time::Instant::now();

    loop {
        tokio::select! {
            msg_opt = read.next() => {
                let msg = match msg_opt {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => {
                        tracing::warn!("CLOB shard {} read error: {}", shard_idx, e);
                        break;
                    }
                    None => {
                        tracing::warn!("CLOB shard {} stream ended", shard_idx);
                        break;
                    }
                };

                // Any message means the connection is alive
                last_activity = tokio::time::Instant::now();

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
                                    token_map,
                                    price_cache,
                                    db_tx,
                                    price_updates_tx,
                                )
                                .await;
                            }
                        }
                    }
                    Message::Pong(_) => {
                        // WebSocket-level pong received — connection confirmed alive
                    }
                    Message::Close(_) => {
                        tracing::info!("CLOB shard {} received close frame", shard_idx);
                        break;
                    }
                    _ => {}
                }
            }
            // Ping sent notification — check if we've gone too long without any response
            _ = ping_rx.recv() => {
                if last_activity.elapsed() > PONG_TIMEOUT {
                    tracing::warn!(
                        "CLOB shard {} pong timeout ({:?} since last activity), disconnecting",
                        shard_idx, last_activity.elapsed()
                    );
                    break;
                }
            }
            // Fallback timeout: if read blocks forever and no pings fire, bail
            _ = tokio::time::sleep(PONG_TIMEOUT) => {
                if last_activity.elapsed() > PONG_TIMEOUT {
                    tracing::warn!(
                        "CLOB shard {} activity timeout ({:?}), disconnecting",
                        shard_idx, last_activity.elapsed()
                    );
                    break;
                }
            }
        }
    }

    ping_handle.abort();
    Ok(())
}

/// Process price_change events: update in-memory cache, broadcast instantly to frontend,
/// and enqueue for batched DB write.
async fn process_price_changes(
    event: &ClobPriceChangeEvent,
    token_map: &TokenMap,
    price_cache: &PriceCache,
    db_tx: &tokio::sync::mpsc::UnboundedSender<LivePrice>,
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

        // Allow 0.0 (no asks) and 1.0 (settled winner) — previously these were rejected
        let best_ask = match &change.best_ask {
            Some(a) => match a.parse::<f64>() {
                Ok(v) if v >= 0.0 && v <= 1.0 => v,
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

        // 1. Update in-memory cache immediately
        price_cache.insert(token_id.clone(), live_price.clone());

        // 2. Broadcast instantly to all connected frontend WebSocket clients
        let broadcast_msg = serde_json::json!({
            "type": "price_update",
            "slug": slug,
            "outcome": outcome,
            "best_ask": best_ask,
            "ts": timestamp,
        })
        .to_string();
        let _ = price_updates_tx.send(broadcast_msg);

        // 3. Enqueue for batched DB write (non-blocking)
        let _ = db_tx.send(live_price);
    }
}

/// Background task: collects LivePrice writes, deduplicates by token_id,
/// and flushes to PostgreSQL every 2 seconds using a batch unnest UPDATE.
async fn db_flush_task(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<LivePrice>,
    pool: sqlx::PgPool,
) {
    // pending: token_id → latest LivePrice (deduped, keep newest)
    let mut pending: HashMap<String, LivePrice> = HashMap::new();
    let mut flush_interval = tokio::time::interval(DB_FLUSH_INTERVAL);
    // Skip the first immediate tick
    flush_interval.tick().await;

    loop {
        tokio::select! {
            // Drain all available messages without blocking
            Some(price) = rx.recv() => {
                pending.insert(price.clob_token_id.clone(), price);
            }
            _ = flush_interval.tick() => {
                if pending.is_empty() {
                    continue;
                }
                let batch: Vec<LivePrice> = pending.drain().map(|(_, v)| v).collect();
                flush_to_db(&pool, batch).await;
            }
        }
    }
}

/// Flush a batch of LivePrice updates to PostgreSQL.
/// Groups by slug, then does a single batch UPDATE per table using unnest arrays.
async fn flush_to_db(pool: &sqlx::PgPool, writes: Vec<LivePrice>) {
    // Aggregate per slug: keep latest up_price and down_price
    let mut by_slug: HashMap<String, (Option<f64>, Option<f64>)> = HashMap::new();
    for lp in &writes {
        let entry = by_slug.entry(lp.slug.clone()).or_insert((None, None));
        if lp.outcome == "up" {
            entry.0 = Some(lp.best_ask);
        } else {
            entry.1 = Some(lp.best_ask);
        }
    }

    let slugs: Vec<String> = by_slug.keys().cloned().collect();
    let up_prices: Vec<Option<f64>> = slugs.iter().map(|s| by_slug[s].0).collect();
    let down_prices: Vec<Option<f64>> = slugs.iter().map(|s| by_slug[s].1).collect();

    // Single batch UPDATE: COALESCE keeps existing price if this batch has no update for that side
    let result = sqlx::query(
        r#"
        UPDATE crypto_markets AS cm
        SET
            up_price    = COALESCE(v.up_price,   cm.up_price),
            down_price  = COALESCE(v.down_price, cm.down_price),
            direction   = CASE
                WHEN COALESCE(v.up_price,   cm.up_price,   0) > COALESCE(v.down_price, cm.down_price, 0) THEN 'up'
                WHEN COALESCE(v.down_price, cm.down_price, 0) > COALESCE(v.up_price,   cm.up_price,   0) THEN 'down'
                ELSE cm.direction
            END,
            updated_at  = NOW()
        FROM unnest($1::text[], $2::float8[], $3::float8[])
             AS v(slug, up_price, down_price)
        WHERE cm.slug = v.slug
        "#,
    )
    .bind(&slugs)
    .bind(&up_prices)
    .bind(&down_prices)
    .execute(pool)
    .await;

    if let Err(e) = result {
        tracing::warn!("CLOB batch DB flush error ({} slugs): {}", slugs.len(), e);
    } else {
        tracing::debug!("CLOB batch flushed {} slug(s) to DB", slugs.len());
    }
}
