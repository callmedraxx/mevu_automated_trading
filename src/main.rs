mod auth;
mod bot_config;
mod chainlink;
mod crypto;
mod mevu_client;
mod trading;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    extract::ws::{WebSocket, WebSocketUpgrade},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
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

    let state = AppState::new(db.clone());

    // Warm the bot config cache from DB
    if let Some(pool) = &db {
        bot_config::service::warm_cache(pool, &state.bot_configs).await;
    }

    // Start MEVU balance SSE streams for all active users
    start_balance_streams(&state).await;

    // Start auto-refresh background task (fetches every hour)
    service::start_auto_refresh(state.clone());

    // Start the trading engine manager (monitors active configs, spawns per-user engine tasks)
    trading::engine::start_engine_manager(state.clone());

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
        // Bot config routes
        .route("/api/bot-config/{user_id}", get(get_bot_config))
        .route("/api/bot-config/{user_id}", axum::routing::put(upsert_bot_config))
        .route("/api/bot-config/{user_id}/start", axum::routing::put(start_bot))
        .route("/api/bot-config/{user_id}/stop", axum::routing::put(stop_bot))
        // Bot config preset routes
        .route("/api/bot-config/{user_id}/presets", get(list_presets_handler).post(save_preset_handler))
        .route("/api/bot-config/{user_id}/presets/{preset_id}", delete(delete_preset_handler))
        // Bot trade history routes
        .route("/api/bot-trades/{user_id}", get(get_bot_trades))
        .route("/api/bot-trades/{user_id}/stats", get(get_bot_trade_stats))
        // BTC market events (with live prices)
        .route("/api/btc-markets", get(get_btc_markets))
        // WebSocket: real-time BTC market price stream
        .route("/ws/btc-markets", get(ws_btc_markets))
        // WebSocket: Chainlink BTC/USD price stream
        .route("/ws/btc-price", get(ws_btc_price))
        // Chainlink BTC price
        .route("/api/chainlink/btc", get(get_chainlink_btc_price))
        // SSE balance stream (forwards from MEVU's Alchemy-backed balance stream)
        .route("/api/balances/stream/{user_id}", get(balance_stream))
        // User dashboard (balance, positions, stats from MEVU + Rust winrate)
        .route("/api/user/{user_id}/balance", get(get_user_balance))
        .route("/api/user/{user_id}/positions", get(get_user_positions))
        .route("/api/user/{user_id}/stats", get(get_user_stats))
        .route("/api/user/{user_id}/dashboard", get(get_user_dashboard))
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

// ─── Bot Config Routes ───────────────────────────────────────────────────────

/// Get the bot config for a user. Returns 404 if no config exists yet.
async fn get_bot_config(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "Database not available"})))
    })?;

    match bot_config::service::get_config(db, &state.bot_configs, &user_id).await {
        Ok(Some(cfg)) => Ok(Json(serde_json::json!({"config": cfg}))),
        Ok(None) => Err((StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "No config found. Save settings to create one."})))),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()})))),
    }
}

/// Create or update bot config for a user (merge semantics — only provided fields are changed).
async fn upsert_bot_config(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
    Json(req): Json<bot_config::types::UpdateBotConfigRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "Database not available"})))
    })?;

    match bot_config::service::upsert_config(db, &state.bot_configs, &user_id, &req).await {
        Ok(cfg) => Ok(Json(serde_json::json!({"config": cfg}))),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()})))),
    }
}

/// Start the trading engine for a user. Validates config first.
async fn start_bot(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "Database not available"})))
    })?;

    // Load config (must exist)
    let cfg = match bot_config::service::get_config(db, &state.bot_configs, &user_id).await {
        Ok(Some(c)) => c,
        Ok(None) => return Err((StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "No config found. Save settings before starting."})))),
        Err(e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()})))),
    };

    // Validate
    if let Err(errors) = bot_config::types::validate_config(&cfg) {
        return Err((StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "Invalid config", "errors": errors}))));
    }

    // Activate
    match bot_config::service::set_active(db, &state.bot_configs, &user_id, true).await {
        Ok(Some(cfg)) => {
            let _ = state.balance_tx.send(serde_json::json!({
                "type": "engine_started",
                "user_id": user_id,
                "message": "Engine started successfully",
            }).to_string());
            Ok(Json(serde_json::json!({"config": cfg, "status": "started"})))
        }
        Ok(None) => Err((StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "Config not found"})))),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()})))),
    }
}

/// Stop the trading engine for a user.
async fn stop_bot(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "Database not available"})))
    })?;

    match bot_config::service::set_active(db, &state.bot_configs, &user_id, false).await {
        Ok(Some(cfg)) => {
            let _ = state.balance_tx.send(serde_json::json!({
                "type": "engine_stopped",
                "user_id": user_id,
                "reason": "Manually stopped by user",
                "balance": 0,
            }).to_string());
            Ok(Json(serde_json::json!({"config": cfg, "status": "stopped"})))
        }
        Ok(None) => Err((StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "Config not found"})))),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()})))),
    }
}

// ─── Bot Config Preset Routes ────────────────────────────────────────────────

/// List all config presets for a user.
async fn list_presets_handler(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "Database not available"})))
    })?;

    match bot_config::service::list_presets(db, &user_id).await {
        Ok(presets) => Ok(Json(serde_json::json!({"presets": presets}))),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()})))),
    }
}

#[derive(serde::Deserialize)]
struct SavePresetRequest {
    name: String,
}

/// Save the current config as a named preset.
async fn save_preset_handler(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
    Json(req): Json<SavePresetRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "Database not available"})))
    })?;

    // Get current config from cache/DB
    let config = match bot_config::service::get_config(db, &state.bot_configs, &user_id).await {
        Ok(Some(c)) => c,
        Ok(None) => return Err((StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "No config found. Save settings before creating a preset."})))),
        Err(e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()})))),
    };

    match bot_config::service::save_preset(db, &user_id, &req.name, &config).await {
        Ok(preset) => Ok(Json(serde_json::json!({"preset": preset}))),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()})))),
    }
}

/// Delete a config preset.
async fn delete_preset_handler(
    State(state): State<Arc<AppState>>,
    Path((user_id, preset_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "Database not available"})))
    })?;

    match bot_config::service::delete_preset(db, &preset_id, &user_id).await {
        Ok(true) => Ok(Json(serde_json::json!({"status": "deleted"}))),
        Ok(false) => Err((StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "Preset not found"})))),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()})))),
    }
}

// ─── Bot Trade History Routes ────────────────────────────────────────────────

/// Query params for trade history
#[derive(serde::Deserialize)]
struct TradeHistoryQuery {
    status: Option<String>,   // "open", "won", "lost", "sold", "error"
    date: Option<String>,     // YYYY-MM-DD, defaults to all
    limit: Option<i64>,       // default 100
}

/// Get trade history for a user.
async fn get_bot_trades(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
    Query(q): Query<TradeHistoryQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "Database not available"})))
    })?;

    let limit = q.limit.unwrap_or(100).min(500);

    // JOIN with crypto_markets to get opening_btc_price, end_date for each trade's market
    let base_select = r#"
        SELECT t.*, cm.opening_btc_price, cm.closing_btc_price, cm.end_date AS market_end_date,
               cm.up_clob_token_id, cm.down_clob_token_id, cm.strike_price
        FROM bot_trades t
        LEFT JOIN crypto_markets cm ON cm.id = t.market_id
    "#;

    let rows = if let Some(ref status) = q.status {
        if let Some(ref date) = q.date {
            let date_pattern = format!("{}%", date);
            sqlx::query(&format!(
                "{} WHERE t.user_id = $1 AND t.status = $2 AND t.created_at::TEXT LIKE $3 ORDER BY t.created_at DESC LIMIT $4",
                base_select
            ))
            .bind(&user_id).bind(status).bind(&date_pattern).bind(limit)
            .fetch_all(db).await
        } else {
            sqlx::query(&format!(
                "{} WHERE t.user_id = $1 AND t.status = $2 ORDER BY t.created_at DESC LIMIT $3",
                base_select
            ))
            .bind(&user_id).bind(status).bind(limit)
            .fetch_all(db).await
        }
    } else if let Some(ref date) = q.date {
        let date_pattern = format!("{}%", date);
        sqlx::query(&format!(
            "{} WHERE t.user_id = $1 AND t.created_at::TEXT LIKE $2 ORDER BY t.created_at DESC LIMIT $3",
            base_select
        ))
        .bind(&user_id).bind(&date_pattern).bind(limit)
        .fetch_all(db).await
    } else {
        sqlx::query(&format!(
            "{} WHERE t.user_id = $1 ORDER BY t.created_at DESC LIMIT $2",
            base_select
        ))
        .bind(&user_id).bind(limit)
        .fetch_all(db).await
    };

    let rows = rows.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()})))
    })?;

    let trades: Vec<serde_json::Value> = rows.iter().map(|row| {
        use sqlx::Row;

        let clob_token_id: String = row.try_get("clob_token_id").unwrap_or_default();
        // Overlay live CLOB price for open trades
        let current_price = state.price_cache.get(&clob_token_id).map(|p| p.best_ask);

        serde_json::json!({
            "id": row.try_get::<String, _>("id").unwrap_or_default(),
            "market_id": row.try_get::<String, _>("market_id").unwrap_or_default(),
            "market_slug": row.try_get::<String, _>("market_slug").unwrap_or_default(),
            "market_title": row.try_get::<String, _>("market_title").unwrap_or_default(),
            "timeframe": row.try_get::<String, _>("timeframe").unwrap_or_default(),
            "side": row.try_get::<String, _>("side").unwrap_or_default(),
            "buy_amount": row.try_get::<f64, _>("buy_amount").unwrap_or(0.0),
            "entry_price": row.try_get::<f64, _>("entry_price").unwrap_or(0.0),
            "exit_price": row.try_get::<Option<f64>, _>("exit_price").ok().flatten(),
            "payout": row.try_get::<Option<f64>, _>("payout").ok().flatten(),
            "pnl": row.try_get::<Option<f64>, _>("pnl").ok().flatten(),
            "status": row.try_get::<String, _>("status").unwrap_or_default(),
            "clob_token_id": clob_token_id,
            "error": row.try_get::<Option<String>, _>("error").ok().flatten(),
            "created_at": row.try_get::<chrono::DateTime<chrono::Utc>, _>("created_at")
                .map(|dt| dt.to_rfc3339()).unwrap_or_default(),
            "opening_btc_price": row.try_get::<Option<f64>, _>("opening_btc_price").ok().flatten(),
            "closing_btc_price": row.try_get::<Option<f64>, _>("closing_btc_price").ok().flatten(),
            "strike_price": row.try_get::<Option<f64>, _>("strike_price").ok().flatten(),
            "market_end_date": row.try_get::<Option<String>, _>("market_end_date").ok().flatten(),
            "current_price": current_price,
        })
    }).collect();

    Ok(Json(serde_json::json!({
        "trades": trades,
        "count": trades.len(),
    })))
}

/// Get trade stats for a user (win/loss/total/pnl).
async fn get_bot_trade_stats(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "Database not available"})))
    })?;

    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let date_pattern = format!("{}%", today);

    // All-time stats
    let all_time = sqlx::query(
        "SELECT
            COUNT(*) FILTER (WHERE status != 'error') as total,
            COUNT(*) FILTER (WHERE status = 'won') as wins,
            COUNT(*) FILTER (WHERE status = 'lost') as losses,
            COUNT(*) FILTER (WHERE status = 'sold') as sold,
            COUNT(*) FILTER (WHERE status = 'open') as open,
            COALESCE(SUM(pnl) FILTER (WHERE pnl IS NOT NULL), 0) as total_pnl
         FROM bot_trades WHERE user_id = $1"
    )
    .bind(&user_id)
    .fetch_one(db)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))))?;

    // Today stats
    let today_stats = sqlx::query(
        "SELECT
            COUNT(*) FILTER (WHERE status != 'error') as total,
            COUNT(*) FILTER (WHERE status = 'won') as wins,
            COUNT(*) FILTER (WHERE status = 'lost') as losses,
            COALESCE(SUM(pnl) FILTER (WHERE pnl IS NOT NULL), 0) as total_pnl
         FROM bot_trades WHERE user_id = $1 AND created_at::TEXT LIKE $2"
    )
    .bind(&user_id)
    .bind(&date_pattern)
    .fetch_one(db)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))))?;

    use sqlx::Row;
    let total: i64 = all_time.try_get("total").unwrap_or(0);
    let wins: i64 = all_time.try_get("wins").unwrap_or(0);
    let losses: i64 = all_time.try_get("losses").unwrap_or(0);
    let sold: i64 = all_time.try_get("sold").unwrap_or(0);
    let open: i64 = all_time.try_get("open").unwrap_or(0);
    let total_pnl: f64 = all_time.try_get("total_pnl").unwrap_or(0.0);

    let today_total: i64 = today_stats.try_get("total").unwrap_or(0);
    let today_wins: i64 = today_stats.try_get("wins").unwrap_or(0);
    let today_losses: i64 = today_stats.try_get("losses").unwrap_or(0);
    let today_pnl: f64 = today_stats.try_get("total_pnl").unwrap_or(0.0);

    let win_rate = if (wins + losses) > 0 {
        (wins as f64 / (wins + losses) as f64) * 100.0
    } else {
        0.0
    };

    Ok(Json(serde_json::json!({
        "all_time": {
            "total": total,
            "wins": wins,
            "losses": losses,
            "sold": sold,
            "open": open,
            "total_pnl": total_pnl,
            "win_rate": win_rate,
        },
        "today": {
            "total": today_total,
            "wins": today_wins,
            "losses": today_losses,
            "total_pnl": today_pnl,
        }
    })))
}

// ─── MEVU Balance SSE Subscriber ───────────────────────────────────────────────

/// Start a background task that subscribes to MEVU's SSE balance stream for a user.
/// MEVU already handles Alchemy webhooks internally; we just consume the resulting events.
pub fn start_mevu_balance_stream(state: Arc<AppState>, user_id: String, privy_user_id: String) {
    tokio::spawn(async move {
        let base = match std::env::var("MEVU_API_URL") {
            Ok(u) => u,
            Err(_) => return,
        };
        let url = format!(
            "{}/api/balances/stream/{}",
            base.trim_end_matches('/'),
            urlencoding::encode(&privy_user_id)
        );

        let mut attempt: u32 = 0;

        loop {
            tracing::info!("Connecting to MEVU balance stream for user={} privy={}", user_id, privy_user_id);

            match state.http_client.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    attempt = 0;
                    let mut stream = resp.bytes_stream();

                    use futures_util::StreamExt;
                    let mut buf = String::new();

                    while let Some(chunk) = stream.next().await {
                        match chunk {
                            Ok(bytes) => {
                                buf.push_str(&String::from_utf8_lossy(&bytes));

                                // SSE protocol: events separated by \n\n
                                while let Some(pos) = buf.find("\n\n") {
                                    let event_str = buf[..pos].to_string();
                                    buf = buf[pos + 2..].to_string();

                                    // Parse "data: {...}" lines
                                    for line in event_str.lines() {
                                        let data = line.strip_prefix("data: ").or_else(|| line.strip_prefix("data:"));
                                        if let Some(json_str) = data {
                                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(json_str) {
                                                // Extract balance from MEVU SSE event
                                                let balance = parsed.get("humanBalance")
                                                    .or_else(|| parsed.get("balance"))
                                                    .and_then(|v| {
                                                        v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                                    });

                                                if let Some(bal) = balance {
                                                    let event_type = parsed.get("type")
                                                        .and_then(|v| v.as_str())
                                                        .unwrap_or("balance_update");

                                                    let tx_hash = parsed.get("txHash")
                                                        .and_then(|v| v.as_str())
                                                        .unwrap_or("");

                                                    let amount = parsed.get("amount")
                                                        .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                                                        .unwrap_or(0.0);

                                                    tracing::info!(
                                                        "MEVU balance stream: user={} type={} balance=${:.2}",
                                                        user_id, event_type, bal
                                                    );

                                                    // Update wallet_balances DB if available
                                                    if let Some(db) = &state.db {
                                                        let _ = sqlx::query(
                                                            "INSERT INTO wallet_balances (proxy_wallet_address, privy_user_id, user_id, balance_raw, balance_human, last_updated_at)
                                                             VALUES ('mevu-stream', $1, $2, $3, $4, NOW())
                                                             ON CONFLICT (proxy_wallet_address) WHERE privy_user_id = $1
                                                             DO NOTHING"
                                                        )
                                                        .bind(&privy_user_id)
                                                        .bind(&user_id)
                                                        .bind(format!("{}", (bal * 1_000_000.0) as i64))
                                                        .bind(bal)
                                                        .execute(db)
                                                        .await;
                                                    }

                                                    // Broadcast to our SSE clients + engine
                                                    let forward = serde_json::json!({
                                                        "type": event_type,
                                                        "user_id": user_id,
                                                        "privy_user_id": privy_user_id,
                                                        "balance": bal,
                                                        "amount": amount,
                                                        "tx_hash": tx_hash,
                                                    });
                                                    let _ = state.balance_tx.send(forward.to_string());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!("MEVU balance stream read error for {}: {}", user_id, e);
                                break;
                            }
                        }
                    }
                }
                Ok(resp) => {
                    tracing::warn!(
                        "MEVU balance stream returned {} for user={}",
                        resp.status(), user_id
                    );
                }
                Err(e) => {
                    tracing::warn!("MEVU balance stream connect failed for {}: {}", user_id, e);
                }
            }

            // Exponential backoff: 1s, 2s, 4s, ... up to 30s
            let delay = std::cmp::min(1000 * 2u64.pow(attempt), 30000);
            attempt = attempt.saturating_add(1);
            tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
        }
    });
}

/// Start MEVU balance streams for all registered users.
/// Called once at startup; also called when engine starts a new user.
pub async fn start_balance_streams(state: &Arc<AppState>) {
    let db = match &state.db {
        Some(db) => db,
        None => return,
    };

    let rows = match sqlx::query("SELECT id, privy_user_id FROM bot_users WHERE is_active = true")
        .fetch_all(db)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("Failed to load users for balance streams: {}", e);
            return;
        }
    };

    use sqlx::Row;
    for row in &rows {
        let user_id: String = row.try_get("id").unwrap_or_default();
        let privy_user_id: String = row.try_get("privy_user_id").unwrap_or_default();
        if !privy_user_id.is_empty() {
            start_mevu_balance_stream(state.clone(), user_id, privy_user_id);
        }
    }

    tracing::info!("Started MEVU balance streams for {} user(s)", rows.len());
}

/// GET /api/balances/stream/{user_id} — SSE stream of real-time balance updates
async fn balance_stream(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
) -> impl IntoResponse {
    use axum::response::sse::{Event, KeepAlive, Sse};
    use futures_util::stream::Stream;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    struct BalanceStream {
        user_id: String,
        rx: tokio::sync::broadcast::Receiver<String>,
        sent_snapshot: bool,
        initial_balance: f64,
        heartbeat: tokio::time::Interval,
    }

    impl Stream for BalanceStream {
        type Item = Result<Event, std::convert::Infallible>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            // Send initial snapshot
            if !self.sent_snapshot {
                self.sent_snapshot = true;
                let snapshot = serde_json::json!({
                    "type": "snapshot",
                    "balance": self.initial_balance,
                });
                return Poll::Ready(Some(Ok(Event::default().data(snapshot.to_string()))));
            }

            // Check for new balance updates
            loop {
                match self.rx.try_recv() {
                    Ok(msg) => {
                        // Filter by user_id
                        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&msg) {
                            if parsed.get("user_id").and_then(|v| v.as_str()) == Some(&self.user_id) {
                                return Poll::Ready(Some(Ok(Event::default().data(msg))));
                            }
                        }
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                        return Poll::Ready(None);
                    }
                }
            }

            // Heartbeat
            if self.heartbeat.poll_tick(cx).is_ready() {
                return Poll::Ready(Some(Ok(Event::default().comment("heartbeat"))));
            }

            // Register waker for broadcast
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    // Get initial balance
    let initial_balance = if let Some(db) = &state.db {
        // Try wallet_balances first, then fall back to MEVU API
        let row = sqlx::query_scalar::<_, f64>(
            "SELECT balance_human FROM wallet_balances WHERE user_id = $1"
        )
        .bind(&user_id)
        .fetch_optional(db)
        .await
        .ok()
        .flatten();

        match row {
            Some(b) => b,
            None => {
                // Fall back to MEVU fetch
                if let Ok(user_row) = sqlx::query("SELECT privy_user_id FROM bot_users WHERE id = $1")
                    .bind(&user_id)
                    .fetch_optional(db)
                    .await
                {
                    if let Some(r) = user_row {
                        use sqlx::Row;
                        let privy_id: String = r.try_get("privy_user_id").unwrap_or_default();
                        mevu_client::fetch_balance(&state.http_client, &privy_id)
                            .await
                            .ok()
                            .and_then(|b| b.human_balance)
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0)
                    } else {
                        0.0
                    }
                } else {
                    0.0
                }
            }
        }
    } else {
        0.0
    };

    let rx = state.balance_tx.subscribe();
    let stream = BalanceStream {
        user_id,
        rx,
        sent_snapshot: false,
        initial_balance,
        heartbeat: tokio::time::interval(tokio::time::Duration::from_secs(30)),
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
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

            let strike_price: Option<f64> = row.try_get("strike_price").ok().flatten();
            let opening_btc_price: Option<f64> = row.try_get("opening_btc_price").ok().flatten();
            let closing_btc_price: Option<f64> = row.try_get("closing_btc_price").ok().flatten();

            let result = if is_resolved {
                // Prefer BTC open vs close comparison (authoritative for settled markets)
                if let (Some(open), Some(close)) = (opening_btc_price, closing_btc_price) {
                    if close > open { Some("up".to_string()) }
                    else if close < open { Some("down".to_string()) }
                    else { None }
                } else {
                    // Fallback: CLOB odds comparison
                    let up = up_price.unwrap_or(0.0);
                    let down = down_price.unwrap_or(0.0);
                    if up > down { Some("up".to_string()) }
                    else if down > up { Some("down".to_string()) }
                    else { None }
                }
            } else {
                None
            };

            // For resolved markets, override prices to 100¢/0¢ based on outcome
            let (up_price, down_price) = if is_resolved {
                match result.as_deref() {
                    Some("up") => (Some(1.0), Some(0.0)),
                    Some("down") => (Some(0.0), Some(1.0)),
                    _ => (up_price, down_price),
                }
            } else {
                (up_price, down_price)
            };

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
    user_id: &str,
) -> Result<auth::types::BotUser, (StatusCode, Json<serde_json::Value>)> {
    auth::service::get_bot_user_by_id(db, user_id)
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
                Json(serde_json::json!({"error": "User not found"})),
            )
        })
}

/// Get USDC balance from MEVU
async fn get_user_balance(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "Database not available"})),
        )
    })?;

    let user = get_bot_user_or_err(db, &user_id).await?;

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
    Path(user_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "Database not available"})),
        )
    })?;

    let user = get_bot_user_or_err(db, &user_id).await?;

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
    Path(user_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "Database not available"})),
        )
    })?;

    let user = get_bot_user_or_err(db, &user_id).await?;

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
    Path(user_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let db = state.db.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "Database not available"})),
        )
    })?;

    let user = get_bot_user_or_err(db, &user_id).await?;

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
        // Bot config table (per-user trading engine configuration)
        "CREATE TABLE IF NOT EXISTS bot_configs (
            user_id TEXT PRIMARY KEY REFERENCES bot_users(id) ON DELETE CASCADE,
            base_amount DOUBLE PRECISION NOT NULL DEFAULT 10.0,
            sell_amount DOUBLE PRECISION,
            max_trade_amount DOUBLE PRECISION,
            max_open_positions INTEGER NOT NULL DEFAULT 1,
            target_direction TEXT,
            target_markets_count INTEGER NOT NULL DEFAULT 1,
            direction_after_count TEXT NOT NULL DEFAULT 'any',
            trade_timeframe TEXT NOT NULL DEFAULT '15m',
            entry_time_before_close INTEGER NOT NULL DEFAULT 60,
            entry_condition TEXT NOT NULL DEFAULT 'lte',
            entry_price_threshold DOUBLE PRECISION NOT NULL DEFAULT 0.50,
            entry_price_max DOUBLE PRECISION,
            exit_strategy TEXT NOT NULL DEFAULT 'hold',
            exit_price_threshold DOUBLE PRECISION,
            loss_action TEXT NOT NULL DEFAULT 'none',
            loss_streak_limit INTEGER NOT NULL DEFAULT 0,
            win_action TEXT NOT NULL DEFAULT 'none',
            win_streak_limit INTEGER NOT NULL DEFAULT 0,
            auto_stop_on_low_balance BOOLEAN NOT NULL DEFAULT TRUE,
            min_balance_threshold DOUBLE PRECISION,
            daily_loss_limit DOUBLE PRECISION,
            is_active BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
        // New columns for bot_configs (handles tables created before these columns existed)
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS allow_retrade BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS max_trade_amount DOUBLE PRECISION DEFAULT NULL",
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS max_open_positions INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS direction_after_count TEXT NOT NULL DEFAULT 'any'",
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS trade_timeframe TEXT NOT NULL DEFAULT '15m'",
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS entry_time_before_close INTEGER NOT NULL DEFAULT 60",
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS entry_price_max DOUBLE PRECISION DEFAULT NULL",
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS daily_loss_limit DOUBLE PRECISION DEFAULT NULL",
        // Bot trades table (trade history recorded by the engine)
        "CREATE TABLE IF NOT EXISTS bot_trades (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL REFERENCES bot_users(id) ON DELETE CASCADE,
            market_id TEXT NOT NULL,
            market_slug TEXT NOT NULL,
            market_title TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            side TEXT NOT NULL,
            buy_amount DOUBLE PRECISION NOT NULL,
            entry_price DOUBLE PRECISION NOT NULL,
            exit_price DOUBLE PRECISION,
            payout DOUBLE PRECISION,
            pnl DOUBLE PRECISION,
            status TEXT NOT NULL DEFAULT 'open',
            clob_token_id TEXT NOT NULL,
            error TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
        "CREATE INDEX IF NOT EXISTS idx_bot_trades_user_id ON bot_trades(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_bot_trades_status ON bot_trades(user_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_bot_trades_market ON bot_trades(user_id, market_id)",
        // Backfill start_time for 1h markets where Gamma API didn't provide it
        "UPDATE crypto_markets SET start_time = TO_CHAR(
            (end_date::TIMESTAMPTZ - INTERVAL '1 hour'), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
         WHERE timeframe = '1h' AND start_time IS NULL AND end_date IS NOT NULL",
        "UPDATE crypto_markets SET start_time = TO_CHAR(
            (end_date::TIMESTAMPTZ - INTERVAL '4 hours'), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
         WHERE timeframe = '4h' AND start_time IS NULL AND end_date IS NOT NULL",
        // Wallet balances table (updated by Alchemy webhooks + after trades)
        "CREATE TABLE IF NOT EXISTS wallet_balances (
            id SERIAL PRIMARY KEY,
            proxy_wallet_address TEXT NOT NULL UNIQUE,
            privy_user_id TEXT NOT NULL,
            user_id TEXT REFERENCES bot_users(id) ON DELETE CASCADE,
            balance_raw TEXT NOT NULL DEFAULT '0',
            balance_human DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
        "CREATE INDEX IF NOT EXISTS idx_wallet_balances_privy ON wallet_balances(privy_user_id)",
        "CREATE INDEX IF NOT EXISTS idx_wallet_balances_user ON wallet_balances(user_id)",
        // Config presets table (saved config snapshots per user)
        "CREATE TABLE IF NOT EXISTS config_presets (
            id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
            user_id TEXT NOT NULL,
            name TEXT NOT NULL,
            config_json JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(user_id, name)
        )",
    ];

    for sql in &statements {
        if let Err(e) = sqlx::query(sql).execute(pool).await {
            tracing::warn!("Migration statement error (may already exist): {}", e);
        }
    }
    tracing::info!("Database migrations applied");
}
