//! Chainlink BTC/USD price stream via Polymarket live-data WebSocket.
//!
//! Endpoint: wss://ws-live-data.polymarket.com/
//! Subscribe: { action: "subscribe", subscriptions: [{ topic: "crypto_prices_chainlink", type: "*", filters: "" }] }
//! Message format: { topic: "crypto_prices_chainlink", type: "update", payload: { symbol: "btc/usd", value: 84321.5, timestamp: 1741780000000 } }

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tracing;

/// Shared atomic storage for BTC/USD price (f64 bits stored as u64)
pub type ChainlinkBtcPrice = Arc<AtomicU64>;

/// Ring-buffer of recent (timestamp_ms, price) points — up to 1 hour of history
pub type BtcPriceHistory = Arc<std::sync::Mutex<Vec<(u64, f64)>>>;

pub fn new_btc_price() -> ChainlinkBtcPrice {
    Arc::new(AtomicU64::new(0))
}

pub fn new_btc_history() -> BtcPriceHistory {
    Arc::new(std::sync::Mutex::new(Vec::new()))
}

/// Read the current BTC price (0.0 if not yet received)
pub fn read_btc_price(price: &ChainlinkBtcPrice) -> f64 {
    f64::from_bits(price.load(Ordering::Relaxed))
}

fn store_btc_price(price: &ChainlinkBtcPrice, val: f64) {
    price.store(val.to_bits(), Ordering::Relaxed);
}

#[derive(Deserialize)]
struct ChainlinkPayload {
    symbol: Option<String>,
    value: Option<f64>,
    full_accuracy_value: Option<String>,
    #[allow(dead_code)]
    timestamp: Option<u64>,
}

#[derive(Deserialize)]
struct ChainlinkMessage {
    topic: Option<String>,
    #[serde(rename = "type")]
    msg_type: Option<String>,
    payload: Option<ChainlinkPayload>,
}

/// Spawn the Chainlink price WebSocket background task.
/// Updates the atomic price, pushes to the history ring-buffer, and broadcasts
/// price_update JSON to all connected /ws/btc-price WebSocket clients.
pub fn start_chainlink_ws(
    btc_price: ChainlinkBtcPrice,
    tx: tokio::sync::broadcast::Sender<String>,
    history: BtcPriceHistory,
) {
    tokio::spawn(async move {
        let url = "wss://ws-live-data.polymarket.com/";
        let subscribe_msg = serde_json::json!({
            "action": "subscribe",
            "subscriptions": [{
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": ""
            }]
        })
        .to_string();

        let mut reconnect_delay = 2u64;

        loop {
            tracing::info!("Chainlink WS: connecting to {}", url);

            match connect_async(url).await {
                Ok((ws_stream, _)) => {
                    tracing::info!("Chainlink WS: connected");
                    reconnect_delay = 2;

                    let (mut write, mut read) = ws_stream.split();

                    if let Err(e) = write.send(Message::Text(subscribe_msg.clone().into())).await {
                        tracing::warn!("Chainlink WS: failed to send subscribe: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                        continue;
                    }

                    let mut ping_interval =
                        tokio::time::interval(tokio::time::Duration::from_secs(30));

                    loop {
                        tokio::select! {
                            msg = read.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if text.is_empty() || text.as_str() == "PING" { continue; }
                                        handle_chainlink_message(&text, &btc_price, &tx, &history);
                                    }
                                    Some(Ok(Message::Ping(data))) => {
                                        let _ = write.send(Message::Pong(data)).await;
                                    }
                                    Some(Ok(Message::Close(_))) | None => {
                                        tracing::warn!("Chainlink WS: connection closed");
                                        break;
                                    }
                                    Some(Err(e)) => {
                                        tracing::warn!("Chainlink WS error: {}", e);
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                            _ = ping_interval.tick() => {
                                let _ = write.send(Message::Ping(bytes::Bytes::new())).await;
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Chainlink WS: failed to connect: {}", e);
                }
            }

            tracing::info!("Chainlink WS: reconnecting in {}s", reconnect_delay);
            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
            reconnect_delay = (reconnect_delay * 2).min(30);
        }
    });
}

fn handle_chainlink_message(
    text: &str,
    btc_price: &ChainlinkBtcPrice,
    tx: &tokio::sync::broadcast::Sender<String>,
    history: &BtcPriceHistory,
) {
    let msg: ChainlinkMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(_) => return,
    };

    if msg.topic.as_deref() != Some("crypto_prices_chainlink") {
        return;
    }

    let msg_type = msg.msg_type.as_deref().unwrap_or("");
    if msg_type != "update" && msg_type != "snapshot" {
        return;
    }

    let payload = match msg.payload {
        Some(p) => p,
        None => return,
    };

    let symbol = payload.symbol.as_deref().unwrap_or("").to_lowercase();
    let is_btc = symbol.contains("btc") || symbol.contains("bitcoin");
    if !is_btc {
        return;
    }

    let price = payload
        .value
        .filter(|v| v.is_finite() && *v > 0.0)
        .or_else(|| {
            payload
                .full_accuracy_value
                .as_deref()
                .and_then(|s| s.parse::<f64>().ok())
                .filter(|v| v.is_finite() && *v > 0.0)
        });

    if let Some(p) = price {
        store_btc_price(btc_price, p);

        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Push to history ring-buffer, trim entries older than 1 hour
        {
            if let Ok(mut h) = history.lock() {
                h.push((ts_ms, p));
                let cutoff = ts_ms.saturating_sub(60 * 60 * 1000);
                if let Some(pos) = h.iter().position(|(t, _)| *t >= cutoff) {
                    if pos > 0 {
                        *h = h.split_off(pos);
                    }
                }
            }
        }

        // Broadcast to connected /ws/btc-price clients
        let broadcast = serde_json::json!({
            "type": "price_update",
            "price": p,
            "timestamp": ts_ms,
        })
        .to_string();
        let _ = tx.send(broadcast);

        tracing::debug!("Chainlink BTC/USD: ${:.2}", p);
    }
}
