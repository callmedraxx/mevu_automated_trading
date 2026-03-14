//! Polymarket SSR opening/closing price fetcher.
//!
//! Fetches BTC market opening/closing prices from Polymarket's Next.js SSR endpoint:
//!   https://polymarket.com/_next/data/{buildId}/en/event/{slug}.json
//!
//! Runs two strategies (same as mevu):
//! 1. Precise tokio timers: scheduled at start_time+2s and end_date+2s for upcoming markets.
//! 2. Sweep every 60s: backfills markets that have already started/ended but are missing prices.

use sqlx::PgPool;
use std::sync::{Arc, Mutex};
use tokio::time::{Duration, sleep};

const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const TIMER_LOOKAHEAD: Duration = Duration::from_secs(5 * 60);
const TIMER_REFRESH: Duration = Duration::from_secs(3 * 60);
const FETCH_DELAY: Duration = Duration::from_secs(2);
const RETRY_INTERVAL: Duration = Duration::from_secs(5);
const MAX_RETRIES: u32 = 5;
const DELAY_BETWEEN_FETCHES: Duration = Duration::from_millis(500);

// ── Build ID cache ────────────────────────────────────────────────────────────

static BUILD_ID: Mutex<Option<String>> = Mutex::new(None);

fn get_cached_build_id() -> Option<String> {
    BUILD_ID.lock().ok()?.clone()
}

fn set_cached_build_id(id: String) {
    if let Ok(mut guard) = BUILD_ID.lock() {
        *guard = Some(id);
    }
}

fn clear_cached_build_id() {
    if let Ok(mut guard) = BUILD_ID.lock() {
        *guard = None;
    }
}

async fn fetch_build_id(client: &reqwest::Client) -> Option<String> {
    let html = client
        .get("https://polymarket.com")
        .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36")
        .header("Accept", "text/html")
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;

    extract_build_id(&html)
}

fn extract_build_id(html: &str) -> Option<String> {
    // Next.js embeds: <script id="__NEXT_DATA__" type="application/json">{"buildId":"XXXX",...}</script>
    let marker = "\"buildId\":\"";
    let start = html.find(marker)? + marker.len();
    let end = html[start..].find('"')? + start;
    let id = &html[start..end];
    if id.is_empty() { None } else { Some(id.to_string()) }
}

async fn get_build_id(client: &reqwest::Client) -> Option<String> {
    if let Some(id) = get_cached_build_id() {
        return Some(id);
    }
    let id = fetch_build_id(client).await?;
    set_cached_build_id(id.clone());
    Some(id)
}

// ── SSR price fetch ───────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SsrPriceData {
    pub open_price: Option<f64>,
    pub close_price: Option<f64>,
}

async fn fetch_from_ssr(
    client: &reqwest::Client,
    slug: &str,
    build_id: &str,
) -> Result<Option<SsrPriceData>, bool> {
    // Returns Ok(data), Ok(None) if no price, Err(true) if build ID stale (404)
    let url = format!(
        "https://polymarket.com/_next/data/{}/en/event/{}.json",
        build_id, slug
    );

    let resp = match client
        .get(&url)
        .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36")
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("[SSR] Request failed for {}: {}", slug, e);
            return Ok(None);
        }
    };

    if resp.status() == 404 {
        tracing::debug!("[SSR] 404 for {} — build ID likely stale", slug);
        return Err(true); // signal: refresh build ID
    }

    if !resp.status().is_success() {
        tracing::warn!("[SSR] HTTP {} for {}", resp.status(), slug);
        return Ok(None);
    }

    let json: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };

    let queries = match json
        .pointer("/pageProps/dehydratedState/queries")
        .and_then(|v| v.as_array())
    {
        Some(q) => q,
        None => return Ok(None),
    };

    for q in queries {
        let key = q.pointer("/queryKey");
        let is_crypto_price = key
            .and_then(|k| k.as_array())
            .map(|arr| {
                arr.first().and_then(|v| v.as_str()) == Some("crypto-prices")
                    && arr.get(1).and_then(|v| v.as_str()) == Some("price")
            })
            .unwrap_or(false);

        if !is_crypto_price {
            continue;
        }

        let data = match q.pointer("/state/data") {
            Some(d) => d,
            None => continue,
        };

        let open_price = data.get("openPrice").and_then(|v| v.as_f64());
        let close_price = data
            .get("closePrice").and_then(|v| v.as_f64())
            .or_else(|| data.get("endPrice").and_then(|v| v.as_f64()))
            .or_else(|| data.get("currentPrice").and_then(|v| v.as_f64()));

        if open_price.is_some() || close_price.is_some() {
            tracing::debug!(
                "[SSR] {} open={:?} close={:?}",
                slug, open_price, close_price
            );
            return Ok(Some(SsrPriceData { open_price, close_price }));
        }
    }

    Ok(None)
}

pub async fn fetch_price_data(
    client: &reqwest::Client,
    slug: &str,
) -> Option<SsrPriceData> {
    let build_id = get_build_id(client).await?;
    match fetch_from_ssr(client, slug, &build_id).await {
        Ok(data) => data,
        Err(_stale) => {
            // Build ID is stale — refresh and retry once
            clear_cached_build_id();
            let new_id = fetch_build_id(client).await?;
            if new_id == build_id {
                return None; // same ID, slug doesn't exist
            }
            set_cached_build_id(new_id.clone());
            fetch_from_ssr(client, slug, &new_id).await.ok().flatten()
        }
    }
}

// ── Store prices in DB ────────────────────────────────────────────────────────

async fn fetch_and_store(
    client: &reqwest::Client,
    pool: &PgPool,
    market_id: &str,
    slug: &str,
    target: &str, // "opening" | "closing"
) {
    for attempt in 0..=MAX_RETRIES {
        if attempt > 0 {
            sleep(RETRY_INTERVAL).await;
        }

        let data = fetch_price_data(client, slug).await;
        let open_price = data.as_ref().and_then(|d| d.open_price);
        let close_price = data.as_ref().and_then(|d| d.close_price);
        let target_price = if target == "closing" { close_price } else { open_price };

        if target_price.is_none() {
            if attempt < MAX_RETRIES {
                tracing::debug!("[SSR] {} price not ready for {}, retry {}/{}", target, slug, attempt + 1, MAX_RETRIES);
                continue;
            }
            tracing::warn!("[SSR] {} price unavailable after {} retries: {}", target, MAX_RETRIES, slug);
            return;
        }

        // Always overwrite — SSR is the authoritative source (Chainlink settlement price)
        let result = if target == "opening" {
            if let Some(p) = open_price {
                sqlx::query(
                    "UPDATE crypto_markets SET opening_btc_price = $1, updated_at = NOW() WHERE id = $2",
                )
                .bind(p).bind(market_id)
                .execute(pool)
                .await
            } else {
                return;
            }
        } else {
            match (open_price, close_price) {
                (Some(o), Some(c)) => sqlx::query(
                    "UPDATE crypto_markets SET opening_btc_price = $1, closing_btc_price = $2, updated_at = NOW() WHERE id = $3",
                )
                .bind(o).bind(c).bind(market_id)
                .execute(pool)
                .await,
                (None, Some(c)) => sqlx::query(
                    "UPDATE crypto_markets SET closing_btc_price = $1, updated_at = NOW() WHERE id = $2",
                )
                .bind(c).bind(market_id)
                .execute(pool)
                .await,
                (Some(o), None) => sqlx::query(
                    "UPDATE crypto_markets SET opening_btc_price = $1, updated_at = NOW() WHERE id = $2",
                )
                .bind(o).bind(market_id)
                .execute(pool)
                .await,
                (None, None) => return,
            }
        };

        match result {
            Ok(_) => tracing::info!("[SSR] Stored {} price for {} open={:?} close={:?}", target, slug, open_price, close_price),
            Err(e) => tracing::warn!("[SSR] DB update failed for {}: {}", slug, e),
        }
        return;
    }
}

// ── Sweep: backfill markets missing prices ────────────────────────────────────

async fn run_sweep(client: Arc<reqwest::Client>, pool: Arc<PgPool>) {
    let now = chrono::Utc::now();
    let sweep_cutoff = now - chrono::Duration::hours(24);
    let recently_started_cutoff = now - chrono::Duration::minutes(10);

    // Ended markets missing closing_price (within last 2 hours)
    let ended = sqlx::query(
        "SELECT id, slug FROM crypto_markets
         WHERE asset = 'bitcoin'
           AND end_date IS NOT NULL
           AND end_date::timestamptz <= $1
           AND end_date::timestamptz >= $2
           AND (closing_btc_price IS NULL OR opening_btc_price IS NULL)
         ORDER BY end_date DESC
         LIMIT 20",
    )
    .bind(now)
    .bind(sweep_cutoff)
    .fetch_all(pool.as_ref())
    .await
    .unwrap_or_default();

    // Markets that started recently and still missing opening_price
    let started = sqlx::query(
        "SELECT id, slug FROM crypto_markets
         WHERE asset = 'bitcoin'
           AND start_time IS NOT NULL
           AND start_time::timestamptz <= $1
           AND start_time::timestamptz >= $2
           AND end_date::timestamptz > $1
           AND opening_btc_price IS NULL
         ORDER BY start_time DESC
         LIMIT 20",
    )
    .bind(now)
    .bind(recently_started_cutoff)
    .fetch_all(pool.as_ref())
    .await
    .unwrap_or_default();

    use std::collections::HashMap;
    let mut to_process: HashMap<String, (String, bool)> = HashMap::new();

    for row in &ended {
        use sqlx::Row;
        let id: String = row.try_get("id").unwrap_or_default();
        let slug: String = row.try_get("slug").unwrap_or_default();
        to_process.insert(slug.clone(), (id, true));
    }
    for row in &started {
        use sqlx::Row;
        let id: String = row.try_get("id").unwrap_or_default();
        let slug: String = row.try_get("slug").unwrap_or_default();
        to_process.entry(slug.clone()).or_insert((id, false));
    }

    for (slug, (id, ended)) in to_process.iter().take(20) {
        sleep(DELAY_BETWEEN_FETCHES).await;
        let target = if *ended { "closing" } else { "opening" };
        fetch_and_store(&client, &pool, id, slug, target).await;
    }
}

// ── Precise timers for upcoming boundaries ────────────────────────────────────

async fn schedule_timers(client: Arc<reqwest::Client>, pool: Arc<PgPool>) {
    let now = chrono::Utc::now();
    let lookahead = now + chrono::Duration::from_std(TIMER_LOOKAHEAD).unwrap_or_default();

    // Markets ending soon without closing_btc_price
    let ending = sqlx::query(
        "SELECT id, slug, end_date FROM crypto_markets
         WHERE asset = 'bitcoin'
           AND end_date::timestamptz > $1
           AND end_date::timestamptz <= $2
           AND closing_btc_price IS NULL
         ORDER BY end_date ASC
         LIMIT 50",
    )
    .bind(now)
    .bind(lookahead)
    .fetch_all(pool.as_ref())
    .await
    .unwrap_or_default();

    // Markets starting soon without opening_btc_price
    let starting = sqlx::query(
        "SELECT id, slug, start_time FROM crypto_markets
         WHERE asset = 'bitcoin'
           AND start_time IS NOT NULL
           AND start_time::timestamptz > $1
           AND start_time::timestamptz <= $2
           AND opening_btc_price IS NULL
         ORDER BY start_time ASC
         LIMIT 50",
    )
    .bind(now)
    .bind(lookahead)
    .fetch_all(pool.as_ref())
    .await
    .unwrap_or_default();

    for row in ending {
        use sqlx::Row;
        let id: String = row.try_get("id").unwrap_or_default();
        let slug: String = row.try_get("slug").unwrap_or_default();
        let end_date: Option<String> = row.try_get("end_date").ok().flatten();

        if let Some(end_str) = end_date {
            if let Ok(end_dt) = chrono::DateTime::parse_from_rfc3339(&end_str) {
                let fire_at = end_dt.with_timezone(&chrono::Utc) + chrono::Duration::seconds(2);
                let delay_ms = (fire_at - chrono::Utc::now()).num_milliseconds().max(0) as u64;
                let c = client.clone();
                let p = pool.clone();
                tokio::spawn(async move {
                    sleep(Duration::from_millis(delay_ms)).await;
                    fetch_and_store(&c, &p, &id, &slug, "closing").await;
                });
            }
        }
    }

    for row in starting {
        use sqlx::Row;
        let id: String = row.try_get("id").unwrap_or_default();
        let slug: String = row.try_get("slug").unwrap_or_default();
        let start_time: Option<String> = row.try_get("start_time").ok().flatten();

        if let Some(start_str) = start_time {
            if let Ok(start_dt) = chrono::DateTime::parse_from_rfc3339(&start_str) {
                let fire_at = start_dt.with_timezone(&chrono::Utc) + chrono::Duration::seconds(2);
                let delay_ms = (fire_at - chrono::Utc::now()).num_milliseconds().max(0) as u64;
                let c = client.clone();
                let p = pool.clone();
                tokio::spawn(async move {
                    sleep(Duration::from_millis(delay_ms)).await;
                    fetch_and_store(&c, &p, &id, &slug, "opening").await;
                });
            }
        }
    }
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Spawn the SSR price cron: precise timers + 60s sweep.
pub fn start_ssr_price_cron(http_client: reqwest::Client, pool: PgPool) {
    let client = Arc::new(http_client);
    let pool = Arc::new(pool);

    tokio::spawn(async move {
        tracing::info!("[SSR] Starting opening/closing price cron (timers + 60s sweep)");

        // Initial run
        schedule_timers(client.clone(), pool.clone()).await;
        run_sweep(client.clone(), pool.clone()).await;

        let mut sweep_tick = tokio::time::interval(SWEEP_INTERVAL);
        let mut timer_tick = tokio::time::interval(TIMER_REFRESH);
        sweep_tick.tick().await; // skip immediate
        timer_tick.tick().await;

        loop {
            tokio::select! {
                _ = sweep_tick.tick() => {
                    run_sweep(client.clone(), pool.clone()).await;
                }
                _ = timer_tick.tick() => {
                    schedule_timers(client.clone(), pool.clone()).await;
                }
            }
        }
    });
}
