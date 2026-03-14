mod auth;
mod chainlink;
mod crypto;
mod mevu_client;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    extract::ws::{WebSocket, WebSocketUpgrade},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use crypto::service::{self, AppState};
use crypto::types::{CryptoMarket, LivePrice, MarketFilter, MarketLivePrices};
use std::net::SocketAddr;
use std::sync::Arc;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi(
    paths(
        get_crypto_markets,
        get_up_markets,
        get_down_markets,
        trigger_refresh,
        get_status,
        get_live_prices,
        get_live_price_by_slug,
    ),
    components(schemas(
        crypto::types::CryptoMarket,
        crypto::types::Direction,
        crypto::types::Timeframe,
        crypto::types::MarketFilter,
        crypto::types::LivePrice,
        crypto::types::MarketLivePrices,
        auth::types::BotUser,
        auth::types::RegisterRequest,
    )),
    tags(
        (name = "crypto-markets", description = "Crypto market endpoints"),
        (name = "auth", description = "Authentication endpoints")
    )
)]
struct ApiDoc;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Connect to PostgreSQL (optional - runs without DB too)
    let db = match std::env::var("DATABASE_URL") {
        Ok(url) => {
            tracing::info!("Connecting to PostgreSQL...");
            match sqlx::PgPool::connect(&url).await {
                Ok(pool) => {
                    tracing::info!("Connected to PostgreSQL");
                    // Run migrations
                    run_migrations(&pool).await;
                    Some(pool)
                }
                Err(e) => {
                    tracing::warn!("Could not connect to PostgreSQL: {}. Running without DB.", e);
                    None
                }
            }
        }
        Err(_) => {
            tracing::info!("No DATABASE_URL set. Running without PostgreSQL.");
            None
        }
    };

    let state = AppState::new(db);

    // Start auto-refresh background task (fetches every hour)
    service::start_auto_refresh(state.clone());

    // Enable CORS for frontend access
    let cors = tower_http::cors::CorsLayer::permissive();

    let app = Router::new()
        .route("/", get(|| async { "Welcome to Automated Trading Engine" }))
        .route("/health", get(health_check))
        // Crypto market routes
        .route("/api/crypto-markets", get(get_crypto_markets))
        .route("/api/crypto-markets/up", get(get_up_markets))
        .route("/api/crypto-markets/down", get(get_down_markets))
        .route("/api/crypto-markets/refresh", post(trigger_refresh))
        .route("/api/crypto-markets/status", get(get_status))
        .route("/api/crypto-markets/live-prices", get(get_live_prices))
        .route("/api/crypto-markets/live-prices/{slug}", get(get_live_price_by_slug))
        // Auth routes
        .route("/api/auth/register", post(register_user))
        .route("/api/auth/me", get(get_current_user))
        .route("/api/auth/proxy-wallet", post(set_proxy_wallet))
        .route("/api/auth/deploy-proxy-wallet", post(deploy_proxy_wallet))
        // BTC market events (with live prices)
        .route("/api/btc-markets", get(get_btc_markets))
        // WebSocket: real-time BTC market price stream
        .route("/ws/btc-markets", get(ws_btc_markets))
        // WebSocket: Chainlink BTC/USD price stream
        .route("/ws/btc-price", get(ws_btc_price))
        // Chainlink BTC price
        .route("/api/chainlink/btc", get(get_chainlink_btc_price))
        // User dashboard (balance, positions, stats from MEVU + Rust winrate)
        .route("/api/user/balance", get(get_user_balance))
        .route("/api/user/positions", get(get_user_positions))
        .route("/api/user/stats", get(get_user_stats))
        .route("/api/user/dashboard", get(get_user_dashboard))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .layer(cors)
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 4500));
    tracing::info!("Server listening on {}", addr);
    tracing::info!("Swagger UI available at http://localhost:4500/swagger-ui/");

    axum::serve(
        tokio::net::TcpListener::bind(addr).await.unwrap(),
        app,
    )
    .await
    .unwrap();
}

async fn health_check() -> &'static str {
    "ok"
}

// ─── Auth Routes ────────────────────────────────────────────────────────────

/// Register or login the bot user via Privy
async fn register_user(
    State(state): State<Arc<AppState>>,
    Json(req): Json<auth::types::RegisterRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "Database not available"})),
        )
    })?;

    let privy_app_id = std::env::var("PRIVY_APP_ID").unwrap_or_default();
    let privy_app_secret = std::env::var("PRIVY_APP_SECRET").unwrap_or_default();

    if privy_app_id.is_empty() || privy_app_secret.is_empty() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "Privy not configured"})),
        ));
    }

    // Verify user exists in Privy
    let privy_user = auth::service::verify_privy_user(
        &state.http_client,
        &privy_app_id,
        &privy_app_secret,
        &req.privy_user_id,
    )
    .await
    .map_err(|e| {
        tracing::error!("Privy verification failed: {}", e);
        (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": format!("Privy verification failed: {}", e)})),
        )
    })?;

    tracing::info!("Privy user verified: {}", privy_user.id);

    // Get or create embedded wallet if not provided
    let wallet_address = if let Some(ref addr) = req.embedded_wallet_address {
        Some(addr.clone())
    } else {
        // Try to create one via Privy API
        match auth::service::create_embedded_wallet(
            &state.http_client,
            &privy_app_id,
            &privy_app_secret,
            &req.privy_user_id,
        )
        .await
        {
            Ok(addr) => Some(addr),
            Err(e) => {
                tracing::warn!("Could not create embedded wallet: {}", e);
                None
            }
        }
    };

    // Create or update user in database
    let mut user = auth::service::get_or_create_user(
        db,
        &req.privy_user_id,
        req.email.as_deref(),
        wallet_address.as_deref(),
    )
    .await
    .map_err(|e| {
        tracing::error!("User creation failed: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("User creation failed: {}", e)})),
        )
    })?;

    // Deploy proxy wallet on first time if not yet deployed (via MEVU Polymarket relayer)
    if user.proxy_wallet_address.is_none() {
        if let Some(ref embedded_addr) = user.embedded_wallet_address {
            tracing::info!(
                "Bot user has no proxy wallet, deploying via MEVU relayer: privy_id={}",
                req.privy_user_id
            );
            match auth::service::deploy_proxy_wallet_via_mevu(
                &state.http_client,
                &req.privy_user_id,
                embedded_addr,
            )
            .await
            {
                Ok(proxy_addr) => {
                    tracing::info!(
                        "Proxy wallet deployed: {} for privy_id={}",
                        proxy_addr,
                        req.privy_user_id
                    );
                    if let Ok(updated) =
                        auth::service::set_proxy_wallet(db, &req.privy_user_id, &proxy_addr).await
                    {
                        user = updated;
                    } else {
                        tracing::error!("Failed to save proxy wallet to DB");
                    }
                }
                Err(e) => {
                    if e.contains("already deployed") {
                        tracing::info!(
                            "Proxy already deployed in MEVU, fetching from profiles: privy_id={}",
                            req.privy_user_id
                        );
                        match auth::service::fetch_proxy_wallet_from_mevu(
                            &state.http_client,
                            &req.privy_user_id,
                        )
                        .await
                        {
                            Ok(Some(proxy_addr)) => {
                                tracing::info!(
                                    "Fetched existing proxy wallet from MEVU: {}",
                                    proxy_addr
                                );
                                if let Ok(updated) =
                                    auth::service::set_proxy_wallet(db, &req.privy_user_id, &proxy_addr)
                                        .await
                                {
                                    user = updated;
                                }
                            }
                            Ok(None) => {
                                tracing::warn!(
                                    "MEVU said already deployed but proxy not in profiles: {}",
                                    e
                                );
                            }
                            Err(fetch_err) => {
                                tracing::warn!(
                                    "Failed to fetch proxy from MEVU profiles: {}",
                                    fetch_err
                                );
                            }
                        }
                    } else {
                        tracing::warn!(
                            "Proxy wallet deployment failed (will retry on next register): {}",
                            e
                        );
                    }
                }
            }
        } else {
            tracing::warn!(
                "Bot user has no embedded wallet - cannot deploy proxy wallet: privy_id={}",
                req.privy_user_id
            );
        }
    }

    Ok(Json(serde_json::json!({
        "user": user,
        "status": "ok",
    })))
}

/// Get the current bot user
async fn get_current_user(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "Database not available"})),
        )
    })?;

    let user = auth::service::get_bot_user(db).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e})),
        )
    })?;

    match user {
        Some(u) => Ok(Json(serde_json::json!({"user": u}))),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "No bot user registered yet"})),
        )),
    }
}

/// Deploy proxy wallet via MEVU (Polymarket relayer) for bot user who has none.
/// Retries deployment if first register attempt failed (e.g. relayer quota).
async fn deploy_proxy_wallet(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "Database not available"})),
        )
    })?;

    let user = auth::service::get_bot_user(db)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "No bot user registered yet"})),
            )
        })?;

    if user.proxy_wallet_address.is_some() {
        return Ok(Json(serde_json::json!({
            "user": user,
            "status": "ok",
            "message": "Proxy wallet already deployed"
        })));
    }

    let embedded_addr = user.embedded_wallet_address.as_ref().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Bot user has no embedded wallet - cannot deploy proxy"})),
        )
    })?;

    let proxy_addr = match auth::service::deploy_proxy_wallet_via_mevu(
        &state.http_client,
        &user.privy_user_id,
        embedded_addr,
    )
    .await
    {
        Ok(addr) => addr,
        Err(e) if e.contains("already deployed") => {
            tracing::info!("Proxy already deployed in MEVU, fetching from profiles");
            match auth::service::fetch_proxy_wallet_from_mevu(&state.http_client, &user.privy_user_id).await {
                Ok(Some(addr)) => addr,
                Ok(None) | Err(_) => {
                    return Err((
                        StatusCode::BAD_GATEWAY,
                        Json(serde_json::json!({"error": format!("Deployment failed: {}", e)})),
                    ));
                }
            }
        }
        Err(e) => {
            tracing::error!("Proxy wallet deployment failed: {}", e);
            return Err((
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"error": format!("Deployment failed: {}", e)})),
            ));
        }
    };

    let updated = auth::service::set_proxy_wallet(db, &user.privy_user_id, &proxy_addr)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        })?;

    Ok(Json(serde_json::json!({
        "user": updated,
        "status": "ok",
        "proxy_wallet_address": proxy_addr,
    })))
}

/// Set the proxy wallet address for the bot user
async fn set_proxy_wallet(
    State(state): State<Arc<AppState>>,
    Json(req): Json<auth::types::SetProxyWalletRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "Database not available"})),
        )
    })?;

    // Get the current bot user
    let user = auth::service::get_bot_user(db)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "No bot user registered yet"})),
            )
        })?;

    let updated = auth::service::set_proxy_wallet(db, &user.privy_user_id, &req.proxy_wallet_address)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        })?;

    Ok(Json(serde_json::json!({
        "user": updated,
        "status": "ok",
    })))
}

// ─── BTC Market Events ─────────────────────────────────────────────────────────

/// Query params for BTC market events
#[derive(serde::Deserialize)]
struct BtcMarketsQuery {
    timeframe: Option<String>,
    date: Option<String>, // YYYY-MM-DD, defaults to today UTC
}

/// BTC market event with live prices overlaid
#[derive(serde::Serialize)]
struct BtcMarketEvent {
    id: String,
    slug: String,
    title: String,
    start_date: Option<String>,
    end_date: Option<String>,
    start_time: Option<String>,
    up_price: Option<f64>,
    down_price: Option<f64>,
    up_clob_token_id: Option<String>,
    down_clob_token_id: Option<String>,
    volume: Option<f64>,
    liquidity: Option<f64>,
    is_active: bool,
    is_resolved: bool,
    series_slug: Option<String>,
    /// BTC price threshold extracted from market title (e.g. 84000.0 for "above $84,000")
    strike_price: Option<f64>,
    /// BTC/USD spot price when market window opened
    opening_btc_price: Option<f64>,
    /// BTC/USD spot price when market window closed/resolved
    closing_btc_price: Option<f64>,
    /// Resolved outcome: "up" or "down" (null if not yet resolved)
    result: Option<String>,
}

/// Build the BTC markets snapshot value (shared by HTTP and WebSocket handlers).
async fn btc_markets_snapshot(
    state: &Arc<AppState>,
    timeframe: &str,
    date: &str,
) -> serde_json::Value {
    let btc_price = chainlink::service::read_btc_price(&state.chainlink_btc);
    let date_pattern = format!("{}%", date);
    let now = chrono::Utc::now();

    // Always prefer DB for complete data (opening/closing prices, strike price).
    // Fall back to in-memory only if DB unavailable.
    let mut events: Vec<BtcMarketEvent> = if let Some(pool) = &state.db {
        let rows = sqlx::query(
            "SELECT id, slug, title, timeframe, asset, direction,
                    up_price, down_price, volume, liquidity, series_slug,
                    start_date, end_date, start_time,
                    up_clob_token_id, down_clob_token_id,
                    strike_price, opening_btc_price, closing_btc_price
             FROM crypto_markets
             WHERE asset = 'bitcoin'
               AND timeframe = $1
               AND (end_date LIKE $2 OR start_time LIKE $2)
             ORDER BY start_time ASC NULLS LAST",
        )
        .bind(timeframe)
        .bind(&date_pattern)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

        rows.iter().map(|row| {
            use sqlx::Row;

            let up_clob: Option<String> = row.try_get("up_clob_token_id").ok().flatten();
            let down_clob: Option<String> = row.try_get("down_clob_token_id").ok().flatten();

            // Overlay live CLOB price if available, else use stored price
            let up_price = up_clob.as_ref()
                .and_then(|id| state.price_cache.get(id).map(|p| p.best_ask))
                .or_else(|| row.try_get("up_price").ok().flatten());
            let down_price = down_clob.as_ref()
                .and_then(|id| state.price_cache.get(id).map(|p| p.best_ask))
                .or_else(|| row.try_get("down_price").ok().flatten());

            let end_date: Option<String> = row.try_get("end_date").ok().flatten();
            // start_time is the actual market window open; start_date is the Gamma fetch time (irrelevant)
            let start_time: Option<String> = row.try_get("start_time").ok().flatten();

            let (is_active, is_resolved) = {
                let start = start_time.as_deref().and_then(parse_dt);
                let end = end_date.as_deref().and_then(parse_dt);
                match (start, end) {
                    (Some(s), Some(e)) => (now >= s && now < e, now >= e),
                    (Some(s), None) => (now >= s, false),
                    _ => (false, false),
                }
            };

            let result = if is_resolved {
                let up = up_price.unwrap_or(0.0);
                let down = down_price.unwrap_or(0.0);
                if up > down { Some("up".to_string()) }
                else if down > up { Some("down".to_string()) }
                else { None }
            } else {
                None
            };

            let strike_price: Option<f64> = row.try_get("strike_price").ok().flatten();
            let opening_btc_price: Option<f64> = row.try_get("opening_btc_price").ok().flatten();
            let closing_btc_price: Option<f64> = row.try_get("closing_btc_price").ok().flatten();

            BtcMarketEvent {
                id: row.try_get("id").unwrap_or_default(),
                slug: row.try_get("slug").unwrap_or_default(),
                title: row.try_get("title").unwrap_or_default(),
                start_date: row.try_get("start_date").ok().flatten(),
                end_date,
                start_time,
                up_price,
                down_price,
                up_clob_token_id: up_clob,
                down_clob_token_id: down_clob,
                volume: row.try_get("volume").ok().flatten(),
                liquidity: row.try_get("liquidity").ok().flatten(),
                is_active,
                is_resolved,
                series_slug: row.try_get("series_slug").ok().flatten(),
                strike_price,
                opening_btc_price,
                closing_btc_price,
                result,
            }
        }).collect()
    } else {
        // Fallback: in-memory markets (no opening/closing prices)
        let markets = state.markets.read().await;
        markets
            .iter()
            .filter(|m| {
                m.timeframe.as_ref().map(|tf| tf.as_str() == timeframe).unwrap_or(false)
                    && (m.start_time.as_deref().map(|d| d.starts_with(date)).unwrap_or(false)
                        || m.end_date.as_deref().map(|d| d.starts_with(date)).unwrap_or(false))
            })
            .map(|m| {
                let up_price = m.up_clob_token_id.as_ref()
                    .and_then(|id| state.price_cache.get(id).map(|p| p.best_ask))
                    .or(m.up_price);
                let down_price = m.down_clob_token_id.as_ref()
                    .and_then(|id| state.price_cache.get(id).map(|p| p.best_ask))
                    .or(m.down_price);

                let (is_active, is_resolved) = parse_market_status(m, &now);

                let result = if is_resolved {
                    let up = up_price.unwrap_or(0.0);
                    let dn = down_price.unwrap_or(0.0);
                    if up > dn { Some("up".to_string()) }
                    else if dn > up { Some("down".to_string()) }
                    else { None }
                } else { None };

                let strike_price = crypto::service::parse_strike_price(&m.title);

                BtcMarketEvent {
                    id: m.id.clone(),
                    slug: m.slug.clone(),
                    title: m.title.clone(),
                    start_date: m.start_date.clone(),
                    end_date: m.end_date.clone(),
                    start_time: m.start_time.clone(),
                    up_price,
                    down_price,
                    up_clob_token_id: m.up_clob_token_id.clone(),
                    down_clob_token_id: m.down_clob_token_id.clone(),
                    volume: m.volume,
                    liquidity: m.liquidity,
                    is_active,
                    is_resolved,
                    series_slug: m.series_slug.clone(),
                    strike_price,
                    opening_btc_price: None,
                    closing_btc_price: None,
                    result,
                }
            })
            .collect()
    };

    // Safety guard: for a given timeframe only ONE market window can be active at a time.
    // If multiple appear active (e.g. dates stored without time component), keep only
    // the one with the most recent start_date (current window).
    let mut found_active = false;
    for event in events.iter_mut().rev() {
        if event.is_active {
            if found_active {
                event.is_active = false;
            } else {
                found_active = true;
            }
        }
    }

    serde_json::json!({
        "type": "snapshot",
        "date": date,
        "timeframe": timeframe,
        "btc_price": if btc_price > 0.0 { Some(btc_price) } else { None::<f64> },
        "count": events.len(),
        "markets": events,
    })
}

/// HTTP: Get BTC market events for a given timeframe and date.
/// Query params: timeframe (5m|15m|1h|4h), date (YYYY-MM-DD, default today UTC).
async fn get_btc_markets(
    State(state): State<Arc<AppState>>,
    Query(q): Query<BtcMarketsQuery>,
) -> Json<serde_json::Value> {
    let timeframe = q.timeframe.as_deref().unwrap_or("15m");
    let date = q.date.clone().unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%d").to_string());
    Json(btc_markets_snapshot(&state, timeframe, &date).await)
}

// ─── WebSocket: real-time BTC market price stream ──────────────────────────

/// WebSocket upgrade handler for real-time BTC market events.
/// Query params: timeframe (5m|15m|1h|4h), date (YYYY-MM-DD, default today UTC).
/// Protocol:
///   → Server sends initial {"type":"snapshot", "markets":[...], "btc_price":...}
///   → Server sends {"type":"price_update","slug":"...","outcome":"up"|"down","best_bid":0.63,"btc_price":...}
///     on every CLOB price change.
///   → Server re-sends snapshot every 60s to catch status transitions.
async fn ws_btc_markets(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    Query(q): Query<BtcMarketsQuery>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws_btc_markets(socket, state, q))
}

async fn handle_ws_btc_markets(
    mut socket: WebSocket,
    state: Arc<AppState>,
    q: BtcMarketsQuery,
) {
    use axum::extract::ws::Message;

    let timeframe = q.timeframe.unwrap_or_else(|| "15m".to_string());
    let date = q.date.unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%d").to_string());

    // Subscribe BEFORE building snapshot so we don't miss any updates that arrive
    // between snapshot build and first recv().
    let mut rx = state.price_updates_tx.subscribe();

    // Send initial full snapshot
    let snapshot = btc_markets_snapshot(&state, &timeframe, &date).await;
    if socket
        .send(Message::Text(snapshot.to_string().into()))
        .await
        .is_err()
    {
        return;
    }

    // Re-send snapshot every 60s to catch market status transitions (upcoming→live→resolved)
    let mut status_tick = tokio::time::interval(tokio::time::Duration::from_secs(60));
    status_tick.tick().await; // skip first immediate tick

    loop {
        tokio::select! {
            // Forward CLOB price updates to the client immediately
            result = rx.recv() => {
                match result {
                    Ok(price_json) => {
                        // Enrich with current BTC spot price
                        let btc = chainlink::service::read_btc_price(&state.chainlink_btc);
                        let msg = if btc > 0.0 {
                            if let Ok(mut v) = serde_json::from_str::<serde_json::Value>(&price_json) {
                                v["btc_price"] = serde_json::json!(btc);
                                v.to_string()
                            } else {
                                price_json
                            }
                        } else {
                            price_json
                        };
                        if socket.send(Message::Text(msg.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Client was too slow; catch up with a full snapshot
                        let snapshot = btc_markets_snapshot(&state, &timeframe, &date).await;
                        if socket.send(Message::Text(snapshot.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }

            // Periodic snapshot so the UI updates active/resolved badges even with no price moves
            _ = status_tick.tick() => {
                let snapshot = btc_markets_snapshot(&state, &timeframe, &date).await;
                if socket.send(Message::Text(snapshot.to_string().into())).await.is_err() {
                    break;
                }
            }

            // Detect client disconnect
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }
}

/// Parse an ISO date/time string to UTC DateTime (RFC3339 or date-only)
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

/// Parse market active/resolved status from start_time / end_date fields
fn parse_market_status(m: &crypto::types::CryptoMarket, now: &chrono::DateTime<chrono::Utc>) -> (bool, bool) {
    let start = m.start_time.as_deref().and_then(parse_dt);
    let end = m.end_date.as_deref().and_then(parse_dt);

    match (start, end) {
        (Some(s), Some(e)) => (*now >= s && *now < e, *now >= e),
        (Some(s), None) => (*now >= s, false),
        _ => (false, false),
    }
}

/// Get latest Chainlink BTC/USD price
async fn get_chainlink_btc_price(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let price = chainlink::service::read_btc_price(&state.chainlink_btc);
    Json(serde_json::json!({
        "symbol": "btc/usd",
        "price": if price > 0.0 { Some(price) } else { None::<f64> },
        "timestamp": chrono::Utc::now().timestamp_millis(),
    }))
}

// ─── WebSocket: Chainlink BTC price stream ──────────────────────────────────

/// WebSocket upgrade handler for real-time Chainlink BTC/USD price.
/// Protocol:
///   → Server sends initial {"type":"price_history","prices":[{"price":84000,"timestamp":...},...]}
///   → Server sends {"type":"price_update","price":84100,"timestamp":...} on each Chainlink tick.
async fn ws_btc_price(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws_btc_price(socket, state))
}

async fn handle_ws_btc_price(mut socket: WebSocket, state: Arc<AppState>) {
    use axum::extract::ws::Message;

    // Subscribe to future updates before sending history (no gap)
    let mut rx = state.chainlink_tx.subscribe();

    // Send price history snapshot
    let history_json = {
        let prices: Vec<serde_json::Value> = if let Ok(h) = state.btc_price_history.lock() {
            h.iter().map(|(ts, p)| serde_json::json!({"price": p, "timestamp": ts})).collect()
        } else {
            vec![]
        };
        serde_json::json!({
            "type": "price_history",
            "symbol": "btc/usd",
            "prices": prices,
        })
        .to_string()
    };

    if socket.send(Message::Text(history_json.into())).await.is_err() {
        return;
    }

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(msg) => {
                        if socket.send(Message::Text(msg.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Catch up: resend history
                        let history_json = {
                            let prices: Vec<serde_json::Value> = if let Ok(h) = state.btc_price_history.lock() {
                                h.iter().map(|(ts, p)| serde_json::json!({"price": p, "timestamp": ts})).collect()
                            } else {
                                vec![]
                            };
                            serde_json::json!({"type":"price_history","symbol":"btc/usd","prices":prices}).to_string()
                        };
                        if socket.send(Message::Text(history_json.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }
}

// ─── User Dashboard (from MEVU + Rust stats) ──────────────────────────────────

async fn get_bot_user_or_err(
    db: &sqlx::PgPool,
) -> Result<auth::types::BotUser, (StatusCode, Json<serde_json::Value>)> {
    auth::service::get_bot_user(db)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "No bot user registered yet"})),
            )
        })
}

/// Get USDC balance from MEVU
async fn get_user_balance(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "Database not available"})),
        )
    })?;

    let user = get_bot_user_or_err(db).await?;

    let balance = mevu_client::fetch_balance(&state.http_client, &user.privy_user_id)
        .await
        .map_err(|e| {
            tracing::warn!("MEVU balance fetch failed: {}", e);
            (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"error": format!("Failed to fetch balance: {}", e)})),
            )
        })?;

    let human_balance = balance.human_balance.clone();
    let human = human_balance
        .as_ref()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);

    Ok(Json(serde_json::json!({
        "balance": human,
        "balanceRaw": balance.balance,
        "humanBalance": human_balance,
    })))
}

/// Get positions from MEVU
async fn get_user_positions(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "Database not available"})),
        )
    })?;

    let user = get_bot_user_or_err(db).await?;

    let positions = mevu_client::fetch_positions(&state.http_client, &user.privy_user_id)
        .await
        .map_err(|e| {
            tracing::warn!("MEVU positions fetch failed: {}", e);
            (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"error": format!("Failed to fetch positions: {}", e)})),
            )
        })?;

    Ok(Json(serde_json::json!({
        "positions": positions,
        "count": positions.len(),
    })))
}

/// Get stats: balance, totalPnl, winRate (Rust), openPositions
async fn get_user_stats(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "Database not available"})),
        )
    })?;

    let user = get_bot_user_or_err(db).await?;

    let (balance_res, positions_res, pnl_res) = tokio::join!(
        mevu_client::fetch_balance(&state.http_client, &user.privy_user_id),
        mevu_client::fetch_positions(&state.http_client, &user.privy_user_id),
        mevu_client::fetch_pnl_current(&state.http_client, &user.privy_user_id),
    );

    let balance = balance_res
        .ok()
        .and_then(|b| b.human_balance)
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);

    let positions = positions_res.unwrap_or_default();
    let open_positions = positions.len() as u32;

    let total_pnl = pnl_res
        .ok()
        .and_then(|p| p.total_pnl)
        .unwrap_or(0.0);

    let win_rate = mevu_client::compute_winrate_from_positions(&positions);

    Ok(Json(serde_json::json!({
        "balance": balance,
        "totalPnl": total_pnl,
        "winRate": win_rate,
        "openPositions": open_positions,
        "totalTrades": positions.len(),
    })))
}

/// Combined dashboard: balance, positions, stats
async fn get_user_dashboard(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "Database not available"})),
        )
    })?;

    let user = get_bot_user_or_err(db).await?;

    let (balance_res, positions_res, pnl_res) = tokio::join!(
        mevu_client::fetch_balance(&state.http_client, &user.privy_user_id),
        mevu_client::fetch_positions(&state.http_client, &user.privy_user_id),
        mevu_client::fetch_pnl_current(&state.http_client, &user.privy_user_id),
    );

    let balance = balance_res
        .ok()
        .and_then(|b| b.human_balance)
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);

    let positions = positions_res.unwrap_or_default();
    let open_positions = positions.len() as u32;

    let total_pnl = pnl_res
        .ok()
        .and_then(|p| p.total_pnl)
        .unwrap_or(0.0);

    let win_rate = mevu_client::compute_winrate_from_positions(&positions);

    Ok(Json(serde_json::json!({
        "balance": balance,
        "positions": positions,
        "stats": {
            "totalPnl": total_pnl,
            "winRate": win_rate,
            "openPositions": open_positions,
            "totalTrades": positions.len(),
        },
    })))
}

// ─── Crypto Market Routes ───────────────────────────────────────────────────

/// List crypto markets with optional filters
///
/// Returns crypto markets filtered by direction (up/down), timeframe (5m, 15m, 1h, 4h, weekly), and asset.
#[utoipa::path(
    get,
    path = "/api/crypto-markets",
    params(
        ("direction" = Option<String>, Query, description = "Filter by direction: 'up' or 'down'"),
        ("timeframe" = Option<String>, Query, description = "Filter by timeframe: '5m', '15m', '1h', '4h', 'weekly'"),
        ("asset" = Option<String>, Query, description = "Filter by asset: 'bitcoin', 'ethereum', 'solana', etc."),
    ),
    responses(
        (status = 200, description = "List of crypto markets", body = Vec<CryptoMarket>)
    ),
    tag = "crypto-markets"
)]
async fn get_crypto_markets(
    State(state): State<Arc<AppState>>,
    Query(filter): Query<MarketFilter>,
) -> Json<Vec<CryptoMarket>> {
    let markets = state.markets.read().await;
    let filtered = service::filter_markets(&markets, &filter);
    Json(filtered)
}

/// List UP-trending crypto markets
///
/// Returns only markets where the up outcome price exceeds the down price.
#[utoipa::path(
    get,
    path = "/api/crypto-markets/up",
    params(
        ("timeframe" = Option<String>, Query, description = "Filter by timeframe: '5m', '15m', '1h', '4h', 'weekly'"),
        ("asset" = Option<String>, Query, description = "Filter by asset: 'bitcoin', 'ethereum', 'solana', etc."),
    ),
    responses(
        (status = 200, description = "List of up-trending crypto markets", body = Vec<CryptoMarket>)
    ),
    tag = "crypto-markets"
)]
async fn get_up_markets(
    State(state): State<Arc<AppState>>,
    Query(mut filter): Query<MarketFilter>,
) -> Json<Vec<CryptoMarket>> {
    filter.direction = Some("up".to_string());
    let markets = state.markets.read().await;
    Json(service::filter_markets(&markets, &filter))
}

/// List DOWN-trending crypto markets
///
/// Returns only markets where the down outcome price exceeds the up price.
#[utoipa::path(
    get,
    path = "/api/crypto-markets/down",
    params(
        ("timeframe" = Option<String>, Query, description = "Filter by timeframe: '5m', '15m', '1h', '4h', 'weekly'"),
        ("asset" = Option<String>, Query, description = "Filter by asset: 'bitcoin', 'ethereum', 'solana', etc."),
    ),
    responses(
        (status = 200, description = "List of down-trending crypto markets", body = Vec<CryptoMarket>)
    ),
    tag = "crypto-markets"
)]
async fn get_down_markets(
    State(state): State<Arc<AppState>>,
    Query(mut filter): Query<MarketFilter>,
) -> Json<Vec<CryptoMarket>> {
    filter.direction = Some("down".to_string());
    let markets = state.markets.read().await;
    Json(service::filter_markets(&markets, &filter))
}

/// Manually trigger a market refresh
///
/// Fetches fresh data from Polymarket Gamma API and updates in-memory state + database.
#[utoipa::path(
    post,
    path = "/api/crypto-markets/refresh",
    responses(
        (status = 200, description = "Refresh result", body = serde_json::Value)
    ),
    tag = "crypto-markets"
)]
async fn trigger_refresh(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    match service::refresh_markets(&state).await {
        Ok(count) => Json(serde_json::json!({
            "status": "ok",
            "markets_refreshed": count
        })),
        Err(e) => Json(serde_json::json!({
            "status": "error",
            "message": e.to_string()
        })),
    }
}

/// Get market status and counts
///
/// Returns counts of total, up, and down markets broken down by timeframe.
#[utoipa::path(
    get,
    path = "/api/crypto-markets/status",
    responses(
        (status = 200, description = "Market status overview", body = serde_json::Value)
    ),
    tag = "crypto-markets"
)]
async fn get_status(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let markets = state.markets.read().await;
    let total = markets.len();
    let up_count = markets.iter().filter(|m| m.direction == Some(crypto::types::Direction::Up)).count();
    let down_count = markets.iter().filter(|m| m.direction == Some(crypto::types::Direction::Down)).count();

    let mut by_timeframe = std::collections::HashMap::new();
    for m in markets.iter() {
        if let Some(tf) = &m.timeframe {
            *by_timeframe.entry(tf.as_str().to_string()).or_insert(0u32) += 1;
        }
    }

    Json(serde_json::json!({
        "total_markets": total,
        "up_markets": up_count,
        "down_markets": down_count,
        "by_timeframe": by_timeframe,
        "has_database": state.db.is_some(),
    }))
}

/// Get all live prices from CLOB WebSocket
///
/// Returns real-time best_bid prices for all tracked tokens.
#[utoipa::path(
    get,
    path = "/api/crypto-markets/live-prices",
    responses(
        (status = 200, description = "All live prices from CLOB WebSocket", body = Vec<LivePrice>)
    ),
    tag = "crypto-markets"
)]
async fn get_live_prices(
    State(state): State<Arc<AppState>>,
) -> Json<Vec<LivePrice>> {
    let prices: Vec<LivePrice> = state
        .price_cache
        .iter()
        .map(|entry| entry.value().clone())
        .collect();
    Json(prices)
}

/// Get live prices for a specific market by slug
///
/// Returns up and down real-time prices for a given market slug.
#[utoipa::path(
    get,
    path = "/api/crypto-markets/live-prices/{slug}",
    params(
        ("slug" = String, Path, description = "Market slug"),
    ),
    responses(
        (status = 200, description = "Live prices for a specific market", body = MarketLivePrices)
    ),
    tag = "crypto-markets"
)]
async fn get_live_price_by_slug(
    State(state): State<Arc<AppState>>,
    Path(slug): Path<String>,
) -> Json<MarketLivePrices> {
    let mut up_price = None;
    let mut down_price = None;

    for entry in state.price_cache.iter() {
        let lp = entry.value();
        if lp.slug == slug {
            match lp.outcome.as_str() {
                "up" => up_price = Some(lp.clone()),
                "down" => down_price = Some(lp.clone()),
                _ => {}
            }
        }
    }

    Json(MarketLivePrices {
        slug,
        up_price,
        down_price,
    })
}

async fn run_migrations(pool: &sqlx::PgPool) {
    let statements = [
        // Crypto markets table
        "CREATE TABLE IF NOT EXISTS crypto_markets (
            id TEXT PRIMARY KEY,
            slug TEXT NOT NULL,
            title TEXT NOT NULL,
            timeframe TEXT,
            asset TEXT,
            direction TEXT,
            up_price DOUBLE PRECISION,
            down_price DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            liquidity DOUBLE PRECISION,
            series_slug TEXT,
            start_date TEXT,
            end_date TEXT,
            start_time TEXT,
            up_clob_token_id TEXT,
            down_clob_token_id TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
        "CREATE INDEX IF NOT EXISTS idx_crypto_markets_timeframe ON crypto_markets(timeframe)",
        "CREATE INDEX IF NOT EXISTS idx_crypto_markets_direction ON crypto_markets(direction)",
        "CREATE INDEX IF NOT EXISTS idx_crypto_markets_asset ON crypto_markets(asset)",
        "CREATE INDEX IF NOT EXISTS idx_crypto_markets_series ON crypto_markets(series_slug)",
        // New columns for BTC price tracking
        "ALTER TABLE crypto_markets ADD COLUMN IF NOT EXISTS strike_price DOUBLE PRECISION",
        "ALTER TABLE crypto_markets ADD COLUMN IF NOT EXISTS opening_btc_price DOUBLE PRECISION",
        "ALTER TABLE crypto_markets ADD COLUMN IF NOT EXISTS closing_btc_price DOUBLE PRECISION",
        // Bot users table
        "CREATE TABLE IF NOT EXISTS bot_users (
            id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
            privy_user_id TEXT UNIQUE NOT NULL,
            email TEXT,
            embedded_wallet_address TEXT,
            proxy_wallet_address TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
        "CREATE INDEX IF NOT EXISTS idx_bot_users_privy_user_id ON bot_users(privy_user_id)",
        "CREATE INDEX IF NOT EXISTS idx_bot_users_embedded_wallet ON bot_users(embedded_wallet_address)",
    ];

    for sql in &statements {
        if let Err(e) = sqlx::query(sql).execute(pool).await {
            tracing::warn!("Migration statement error (may already exist): {}", e);
        }
    }
    tracing::info!("Database migrations applied");
}
