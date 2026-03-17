use crate::chainlink::service::{read_btc_price, ChainlinkBtcPrice, BtcPriceHistory, new_btc_price, new_btc_history, start_chainlink_ws};
use crate::crypto::cache::CryptoMarketsCache;
use crate::crypto::clob_websocket::{self, PriceCache, TokenMap};
use crate::crypto::gamma_client::fetch_all_crypto_events;
use crate::crypto::types::*;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use tokio::time::{self, Duration};

const REFRESH_INTERVAL: Duration = Duration::from_secs(60 * 60); // 1 hour
const LIFECYCLE_INTERVAL: Duration = Duration::from_secs(30); // check market transitions every 30s

/// Known timeframe tag slugs from Polymarket
const TIMEFRAME_TAGS: &[&str] = &["5m", "5min", "15m", "15min", "1h", "1hour", "4h", "4hour", "daily", "weekly", "monthly", "yearly"];

/// Shared application state
pub struct AppState {
    pub markets: RwLock<Vec<CryptoMarket>>,
    pub cache: CryptoMarketsCache,
    pub http_client: reqwest::Client,
    pub db: Option<sqlx::PgPool>,
    /// Maps clob_token_id → (slug, outcome) for BTC markets only
    pub token_map: TokenMap,
    /// Real-time prices from CLOB WebSocket: clob_token_id → LivePrice
    pub price_cache: PriceCache,
    /// Chainlink BTC/USD price (updated from Polymarket live-data WS)
    pub chainlink_btc: ChainlinkBtcPrice,
    /// Broadcast channel: sends JSON price-update strings to all connected WebSocket clients
    pub price_updates_tx: tokio::sync::broadcast::Sender<String>,
    /// Broadcast channel: sends Chainlink BTC price JSON to /ws/btc-price clients
    pub chainlink_tx: tokio::sync::broadcast::Sender<String>,
    /// Ring-buffer of recent BTC price history (last 1 hour)
    pub btc_price_history: BtcPriceHistory,
    /// Notify handle: trigger CLOB WebSocket to immediately reconnect with latest token map
    pub clob_reconnect: Arc<Notify>,
    /// Per-user bot config cache: user_id → BotConfig (read by trading engine on every tick)
    pub bot_configs: crate::bot_config::service::ConfigCache,
    /// Broadcast channel for balance updates: sends JSON { user_id, privy_user_id, balance, type } to SSE clients
    pub balance_tx: tokio::sync::broadcast::Sender<String>,
}

impl AppState {
    pub fn new(db: Option<sqlx::PgPool>) -> Arc<Self> {
        let chainlink_btc = new_btc_price();
        let btc_price_history = new_btc_history();
        let (chainlink_tx, _) = tokio::sync::broadcast::channel::<String>(512);
        start_chainlink_ws(chainlink_btc.clone(), chainlink_tx.clone(), btc_price_history.clone());
        let (price_updates_tx, _) = tokio::sync::broadcast::channel(512);
        let (balance_tx, _) = tokio::sync::broadcast::channel(256);

        Arc::new(Self {
            markets: RwLock::new(Vec::new()),
            cache: CryptoMarketsCache::new(),
            http_client: reqwest::Client::new(),
            db,
            token_map: TokenMap::default(),
            price_cache: PriceCache::default(),
            chainlink_btc,
            price_updates_tx,
            chainlink_tx,
            btc_price_history,
            clob_reconnect: Arc::new(Notify::new()),
            bot_configs: crate::bot_config::service::ConfigCache::default(),
            balance_tx,
        })
    }
}

/// Extract timeframe from event tags
fn extract_timeframe(tags: &[GammaTag]) -> Option<Timeframe> {
    for tag in tags {
        let slug = tag.slug.to_lowercase();
        if TIMEFRAME_TAGS.contains(&slug.as_str()) {
            if let Some(tf) = Timeframe::from_str(&slug) {
                return Some(tf);
            }
        }
        if let Some(label) = &tag.label {
            let label_lower = label.to_lowercase();
            if let Some(tf) = Timeframe::from_str(&label_lower) {
                return Some(tf);
            }
        }
    }
    None
}

/// Extract asset from event tags — only returns "bitcoin"
fn extract_asset(tags: &[GammaTag]) -> Option<String> {
    for tag in tags {
        let slug = tag.slug.to_lowercase();
        if slug == "bitcoin" {
            return Some(slug);
        }
    }
    None
}

/// Parse outcome prices from a GammaMarket
fn parse_outcome_prices(market: &GammaMarket) -> (Option<f64>, Option<f64>) {
    let prices = match &market.outcome_prices {
        Some(v) => v,
        None => return (None, None),
    };

    let arr = if let Some(s) = prices.as_str() {
        serde_json::from_str::<Vec<String>>(s).ok()
    } else if let Some(arr) = prices.as_array() {
        Some(arr.iter().filter_map(|v| v.as_str().map(String::from).or_else(|| Some(v.to_string()))).collect())
    } else {
        None
    };

    match arr {
        Some(vals) if vals.len() >= 2 => {
            let up = vals[0].trim_matches('"').parse::<f64>().ok();
            let down = vals[1].trim_matches('"').parse::<f64>().ok();
            (up, down)
        }
        _ => (None, None),
    }
}

/// Parse CLOB token IDs from a GammaMarket
fn parse_clob_token_ids(market: &GammaMarket) -> (Option<String>, Option<String>) {
    let ids = match &market.clob_token_ids {
        Some(v) => v,
        None => return (None, None),
    };

    let arr = if let Some(s) = ids.as_str() {
        serde_json::from_str::<Vec<String>>(s).ok()
    } else if let Some(arr) = ids.as_array() {
        Some(arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
    } else {
        None
    };

    match arr {
        Some(vals) if vals.len() >= 2 => (Some(vals[0].clone()), Some(vals[1].clone())),
        _ => (None, None),
    }
}

/// Determine dominant direction from prices
fn determine_direction(up_price: Option<f64>, down_price: Option<f64>) -> Option<Direction> {
    match (up_price, down_price) {
        (Some(up), Some(down)) => {
            if up > down { Some(Direction::Up) }
            else if down > up { Some(Direction::Down) }
            else { None }
        }
        _ => None,
    }
}

/// Parse BTC strike price (USD threshold) from market title.
/// Handles: "$84,000", "$84k", "$84.5k", "$84500"
pub fn parse_strike_price(title: &str) -> Option<f64> {
    let start = title.find('$')?;
    let rest = &title[start + 1..];
    let mut num = String::new();
    let mut is_k = false;

    for ch in rest.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            num.push(ch);
        } else if ch == ',' {
            // skip thousands separators
        } else if (ch == 'k' || ch == 'K') && !num.is_empty() {
            is_k = true;
            break;
        } else if !num.is_empty() {
            break;
        }
    }

    if num.is_empty() { return None; }
    let val: f64 = num.parse().ok()?;
    Some(if is_k { val * 1000.0 } else { val })
}

/// Transform GammaEvents into CryptoMarkets — Bitcoin only
pub fn transform_events(events: Vec<GammaEvent>) -> Vec<CryptoMarket> {
    let mut markets = Vec::new();

    for event in events {
        let tags = event.tags.as_deref().unwrap_or(&[]);
        let timeframe = extract_timeframe(tags);

        let tf = match &timeframe {
            Some(tf) => tf.clone(),
            None => continue,
        };

        match tf {
            Timeframe::FiveMin | Timeframe::FifteenMin | Timeframe::OneHour | Timeframe::FourHour | Timeframe::Weekly => {}
        }

        let asset = extract_asset(tags);

        // Only process bitcoin markets
        if asset.as_deref() != Some("bitcoin") {
            continue;
        }

        let first_market = event.markets.as_ref().and_then(|m| m.first());
        let (up_price, down_price) = first_market.map(|m| parse_outcome_prices(m)).unwrap_or((None, None));
        let (up_token, down_token) = first_market.map(|m| parse_clob_token_ids(m)).unwrap_or((None, None));
        let direction = determine_direction(up_price, down_price);

        // Derive start_time from end_date if missing (e.g., 1h markets from Gamma API)
        let start_time = event.start_time.clone().or_else(|| {
            let end_str = event.end_date.as_ref()?;
            let end_dt = parse_dt(end_str)?;
            let duration = match tf {
                Timeframe::FiveMin => chrono::Duration::minutes(5),
                Timeframe::FifteenMin => chrono::Duration::minutes(15),
                Timeframe::OneHour => chrono::Duration::hours(1),
                Timeframe::FourHour => chrono::Duration::hours(4),
                Timeframe::Weekly => chrono::Duration::weeks(1),
            };
            Some((end_dt - duration).to_rfc3339())
        });

        markets.push(CryptoMarket {
            id: event.id.clone(),
            slug: event.slug.clone(),
            title: event.title.clone(),
            timeframe: Some(tf),
            asset,
            direction,
            up_price,
            down_price,
            volume: event.volume,
            liquidity: event.liquidity,
            series_slug: event.series_slug.clone(),
            start_date: event.start_date.clone(),
            end_date: event.end_date.clone(),
            start_time,
            up_clob_token_id: up_token,
            down_clob_token_id: down_token,
        });
    }

    markets
}

/// Filter markets by direction and/or timeframe
pub fn filter_markets(markets: &[CryptoMarket], filter: &MarketFilter) -> Vec<CryptoMarket> {
    markets
        .iter()
        .filter(|m| {
            if let Some(dir) = &filter.direction {
                let target = match dir.to_lowercase().as_str() {
                    "up" => Direction::Up,
                    "down" => Direction::Down,
                    _ => return false,
                };
                if m.direction.as_ref() != Some(&target) {
                    return false;
                }
            }

            if let Some(tf_str) = &filter.timeframe {
                if let Some(target_tf) = Timeframe::from_str(tf_str) {
                    if m.timeframe.as_ref() != Some(&target_tf) {
                        return false;
                    }
                } else {
                    return false;
                }
            }

            if let Some(asset) = &filter.asset {
                if m.asset.as_deref() != Some(asset.to_lowercase().as_str()) {
                    return false;
                }
            }

            true
        })
        .cloned()
        .collect()
}

/// Refresh markets: fetch BTC markets from Gamma API, store in DB, rebuild token map
pub async fn refresh_markets(state: &Arc<AppState>) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!("Refreshing BTC markets from Polymarket...");

    let events = fetch_all_crypto_events(&state.http_client).await?;
    // transform_events already filters to bitcoin-only
    let btc_markets = transform_events(events);
    let count = btc_markets.len();

    tracing::info!("Fetched {} BTC markets", count);

    if let Some(pool) = &state.db {
        if let Err(e) = store_markets_in_db(pool, &btc_markets).await {
            tracing::error!("Failed to store BTC markets in DB: {}", e);
        }
    }

    clob_websocket::rebuild_token_map(&state.token_map, &btc_markets);

    // Trigger immediate CLOB reconnect so new tokens are subscribed right away
    state.clob_reconnect.notify_one();

    {
        let mut lock = state.markets.write().await;
        *lock = btc_markets;
    }

    state.cache.clear();

    Ok(count)
}

/// Store BTC markets in PostgreSQL (upsert). Sets strike_price from title.
/// Does NOT overwrite opening_btc_price / closing_btc_price (set by lifecycle monitor).
async fn store_markets_in_db(pool: &sqlx::PgPool, markets: &[CryptoMarket]) -> Result<(), sqlx::Error> {
    for market in markets {
        let strike = parse_strike_price(&market.title);

        sqlx::query(
            r#"
            INSERT INTO crypto_markets (
                id, slug, title, timeframe, asset, direction,
                up_price, down_price, volume, liquidity,
                series_slug, start_date, end_date, start_time,
                up_clob_token_id, down_clob_token_id, strike_price, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, NOW())
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                direction = EXCLUDED.direction,
                up_price = COALESCE(EXCLUDED.up_price, crypto_markets.up_price),
                down_price = COALESCE(EXCLUDED.down_price, crypto_markets.down_price),
                volume = EXCLUDED.volume,
                liquidity = EXCLUDED.liquidity,
                series_slug = EXCLUDED.series_slug,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                start_time = EXCLUDED.start_time,
                up_clob_token_id = EXCLUDED.up_clob_token_id,
                down_clob_token_id = EXCLUDED.down_clob_token_id,
                strike_price = COALESCE(EXCLUDED.strike_price, crypto_markets.strike_price),
                updated_at = NOW()
            "#,
        )
        .bind(&market.id)
        .bind(&market.slug)
        .bind(&market.title)
        .bind(market.timeframe.as_ref().map(|t| t.as_str()))
        .bind(&market.asset)
        .bind(market.direction.as_ref().map(|d| d.to_string()))
        .bind(market.up_price)
        .bind(market.down_price)
        .bind(market.volume)
        .bind(market.liquidity)
        .bind(&market.series_slug)
        .bind(&market.start_date)
        .bind(&market.end_date)
        .bind(&market.start_time)
        .bind(&market.up_clob_token_id)
        .bind(&market.down_clob_token_id)
        .bind(strike)
        .execute(pool)
        .await?;
    }

    tracing::info!("Stored {} BTC markets in PostgreSQL", markets.len());
    Ok(())
}

/// Parse a date/time string to UTC DateTime (RFC3339 or date-only)
fn parse_dt(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .or_else(|| {
            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .ok()
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .map(|ndt| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc))
        })
}

/// Background task: records opening/closing BTC prices at market lifecycle transitions.
/// Runs every 30 seconds. Sets opening_btc_price when market becomes active,
/// closing_btc_price when market resolves.
async fn market_lifecycle_monitor(state: Arc<AppState>) {
    let mut interval = time::interval(LIFECYCLE_INTERVAL);
    loop {
        interval.tick().await;

        let btc_price = read_btc_price(&state.chainlink_btc);
        if btc_price <= 0.0 {
            continue;
        }

        let pool = match &state.db {
            Some(p) => p,
            None => continue,
        };

        let now = chrono::Utc::now();

        // Fetch BTC markets that still need opening or closing BTC price
        let rows = match sqlx::query(
            "SELECT id, start_time, end_date, opening_btc_price, closing_btc_price
             FROM crypto_markets
             WHERE asset = 'bitcoin'
               AND (opening_btc_price IS NULL OR closing_btc_price IS NULL)",
        )
        .fetch_all(pool)
        .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("Lifecycle monitor DB query failed: {}", e);
                continue;
            }
        };

        for row in &rows {
            use sqlx::Row;
            let id: String = row.try_get("id").unwrap_or_default();
            let start_time: Option<String> = row.try_get("start_time").ok().flatten();
            let end_date: Option<String> = row.try_get("end_date").ok().flatten();
            let opening: Option<f64> = row.try_get("opening_btc_price").ok().flatten();
            let closing: Option<f64> = row.try_get("closing_btc_price").ok().flatten();

            let start = start_time.as_deref().and_then(parse_dt);
            let end = end_date.as_deref().and_then(parse_dt);

            // Set opening price when market is active (and not yet set)
            if opening.is_none() {
                if let (Some(s), Some(e)) = (&start, &end) {
                    if now >= *s && now < *e {
                        let _ = sqlx::query(
                            "UPDATE crypto_markets SET opening_btc_price = $1, updated_at = NOW() WHERE id = $2",
                        )
                        .bind(btc_price)
                        .bind(&id)
                        .execute(pool)
                        .await;
                        tracing::info!("Recorded opening BTC price ${:.0} for market {}", btc_price, id);
                    }
                }
            }

            // Set closing price when market resolves (and not yet set)
            if closing.is_none() {
                if let Some(e) = &end {
                    if now >= *e {
                        let _ = sqlx::query(
                            "UPDATE crypto_markets SET closing_btc_price = $1, updated_at = NOW() WHERE id = $2",
                        )
                        .bind(btc_price)
                        .bind(&id)
                        .execute(pool)
                        .await;
                        tracing::info!("Recorded closing BTC price ${:.0} for market {}", btc_price, id);
                    }
                }
            }
        }
    }
}

/// Start the auto-refresh background task + CLOB WebSocket + lifecycle monitor
pub fn start_auto_refresh(state: Arc<AppState>) {
    // Start CLOB WebSocket for real-time prices (instant frontend broadcast, batched DB writes)
    clob_websocket::start_clob_websocket(
        state.token_map.clone(),
        state.price_cache.clone(),
        state.db.clone(),
        state.price_updates_tx.clone(),
        state.clob_reconnect.clone(),
    );

    // Start market lifecycle monitor (records opening/closing BTC prices from Chainlink)
    let state_lm = state.clone();
    tokio::spawn(async move {
        market_lifecycle_monitor(state_lm).await;
    });

    // Start SSR price cron (fetches authoritative opening/closing prices from Polymarket SSR)
    if let Some(pool) = &state.db {
        crate::crypto::ssr_prices::start_ssr_price_cron(
            state.http_client.clone(),
            pool.clone(),
        );
    }

    tokio::spawn(async move {
        match refresh_markets(&state).await {
            Ok(count) => tracing::info!("Initial refresh: {} BTC markets loaded", count),
            Err(e) => tracing::error!("Initial refresh failed: {}", e),
        }

        let mut interval = time::interval(REFRESH_INTERVAL);
        interval.tick().await;

        loop {
            interval.tick().await;
            match refresh_markets(&state).await {
                Ok(count) => tracing::info!("Auto-refresh: {} BTC markets updated", count),
                Err(e) => tracing::error!("Auto-refresh failed: {}", e),
            }
        }
    });
}
