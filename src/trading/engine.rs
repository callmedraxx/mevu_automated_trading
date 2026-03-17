//! Trading engine: per-user background task that follows bot_config to place trades.
//!
//! Architecture:
//! - One `EngineManager` owns a DashMap<user_id, CancellationToken>.
//! - A scanner task runs every 2s, checking which users have is_active=true in the config cache.
//!   It spawns/cancels per-user engine tasks accordingly.
//! - Each per-user task runs a tight loop: find market → evaluate conditions → execute trade.

use crate::bot_config::types::BotConfig;
use crate::crypto::service::AppState;
use crate::trading::executor;
use crate::trading::types::*;
use dashmap::DashMap;
use sqlx::PgPool;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Manages per-user engine tasks. Shared across the app.
pub type EngineHandles = Arc<DashMap<String, CancellationToken>>;

/// Start the engine manager: a background task that monitors bot_configs and
/// spawns/cancels per-user engine loops.
pub fn start_engine_manager(state: Arc<AppState>) {
    let handles: EngineHandles = Arc::new(DashMap::new());

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3));
        loop {
            interval.tick().await;
            sync_engine_tasks(&state, &handles).await;
        }
    });
}

/// Compare active configs with running engine tasks. Spawn missing, cancel stale.
async fn sync_engine_tasks(state: &Arc<AppState>, handles: &EngineHandles) {
    let pool = match &state.db {
        Some(p) => p,
        None => return,
    };

    // Collect all user IDs that should be running
    let mut active_users: Vec<String> = Vec::new();
    for entry in state.bot_configs.iter() {
        if entry.value().is_active {
            active_users.push(entry.key().clone());
        }
    }

    // Stop engines for users no longer active
    let running_users: Vec<String> = handles.iter().map(|e| e.key().clone()).collect();
    for user_id in &running_users {
        if !active_users.contains(user_id) {
            if let Some((_, token)) = handles.remove(user_id) {
                tracing::info!("Stopping engine for user {}", user_id);
                token.cancel();
            }
        }
    }

    // Start engines for newly active users
    for user_id in &active_users {
        if handles.contains_key(user_id) {
            continue; // already running
        }

        // Look up the user's privy_user_id from bot_users table
        let privy_id = match get_privy_user_id(pool, user_id).await {
            Some(id) => id,
            None => {
                tracing::warn!("Cannot start engine for user {}: no privy_user_id found", user_id);
                continue;
            }
        };

        let token = CancellationToken::new();
        handles.insert(user_id.clone(), token.clone());

        // Start MEVU balance SSE stream for this user (if not already running, it's idempotent)
        crate::start_mevu_balance_stream(state.clone(), user_id.clone(), privy_id.clone());

        let state = state.clone();
        let user_id = user_id.clone();
        let pool = pool.clone();

        tokio::spawn(async move {
            tracing::info!("Engine started for user {} (privy={})", user_id, privy_id);
            run_user_engine(state, pool, user_id.clone(), privy_id, token).await;
            tracing::info!("Engine stopped for user {}", user_id);
        });
    }
}

/// Look up privy_user_id from bot_users by the bot_configs user_id (which is bot_users.id).
async fn get_privy_user_id(pool: &PgPool, user_id: &str) -> Option<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT privy_user_id FROM bot_users WHERE id = $1"
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
}

/// Per-user engine loop. Runs until cancelled or config.is_active becomes false.
async fn run_user_engine(
    state: Arc<AppState>,
    pool: PgPool,
    user_id: String,
    privy_user_id: String,
    cancel: CancellationToken,
) {
    // Read initial config to set correct base amount
    let initial_base = state.bot_configs.get(&user_id)
        .map(|c| c.value().base_amount)
        .unwrap_or(10.0);
    let mut engine_state = UserEngineState::new(initial_base);
    let mut last_day = chrono::Utc::now().date_naive();

    // Load market IDs that already have a successful (non-error) trade today
    // to avoid double-entry on restart. Error trades are excluded so the engine can retry.
    if let Ok(traded_ids) = load_traded_market_ids_today(&pool, &user_id).await {
        engine_state.traded_market_ids = traded_ids;
    }

    // Reconstruct streak state from today's resolved trades, then replay
    // streak adjustments on trade amount so we resume at the correct bet size
    {
        let config = state.bot_configs.get(&user_id).map(|c| c.value().clone());
        reconstruct_streak_state(&pool, &user_id, &mut engine_state).await;
        if let Some(cfg) = config {
            replay_streak_amount(&cfg, &mut engine_state);
        }
    }

    // Initial balance fetch
    refresh_balance(&state.http_client, &privy_user_id, &mut engine_state).await;

    // Subscribe to real-time balance updates from Alchemy webhooks
    let mut balance_rx = state.balance_tx.subscribe();

    // Main loop: tick every 2 seconds
    let mut tick = tokio::time::interval(tokio::time::Duration::from_secs(2));

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tick.tick() => {},
            // Immediately update cached balance when Alchemy webhook fires
            result = balance_rx.recv() => {
                if let Ok(msg) = result {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&msg) {
                        if parsed.get("user_id").and_then(|v| v.as_str()) == Some(&user_id) {
                            if let Some(new_bal) = parsed.get("balance").and_then(|v| v.as_f64()) {
                                engine_state.cached_balance = new_bal;
                                engine_state.last_balance_refresh = Some(chrono::Utc::now());
                                tracing::info!(
                                    "Engine user={}: balance updated via webhook: ${:.2}",
                                    user_id, new_bal
                                );
                            }
                        }
                    }
                }
                continue; // Don't run the tick logic, just update balance and wait for next event
            }
        }

        // Read config from cache (fast DashMap read)
        let config = match state.bot_configs.get(&user_id) {
            Some(entry) => entry.value().clone(),
            None => break, // config removed
        };

        // Check if still active
        if !config.is_active {
            break;
        }

        // Day rollover: reset daily state
        let today = chrono::Utc::now().date_naive();
        if today != last_day {
            tracing::info!("Day rollover for user {}: resetting daily state", user_id);
            engine_state.reset_daily(config.base_amount);
            last_day = today;
        }

        // Sync trade amount from config:
        // - If current amount went to 0 or below (shouldn't happen, safety net)
        // - If no active streaks and user changed base_amount, pick it up immediately
        if engine_state.current_trade_amount <= 0.0 {
            engine_state.current_trade_amount = config.base_amount;
        } else if engine_state.win_streak == 0 && engine_state.loss_streak == 0 {
            // No active streak: always use latest base_amount from config
            engine_state.current_trade_amount = config.base_amount;
        }

        // Periodic balance refresh (every 60s)
        let should_refresh_balance = engine_state.last_balance_refresh
            .map(|t| chrono::Utc::now() - t > chrono::Duration::seconds(60))
            .unwrap_or(true);
        if should_refresh_balance {
            refresh_balance(&state.http_client, &privy_user_id, &mut engine_state).await;
        }

        // Balance safety checks — only enforced when there is NO active open trade.
        // While a trade is open, balance is temporarily reduced; it may recover on win.
        // We only care about balance when the engine is actually about to place the next trade.
        //
        // A trade is considered "active" only if the market end time (parsed from slug) has NOT
        // yet passed. Past-end trades are either in the redeem retry loop or were manually
        // sold/redeemed outside the bot — they should not block the balance check indefinitely.
        let has_open_trade = has_active_open_trade(&pool, &user_id).await;
        if !has_open_trade {
            // Safety: auto-stop on low balance
            if config.auto_stop_on_low_balance && engine_state.cached_balance < engine_state.current_trade_amount {
                let reason = format!(
                    "Insufficient balance (${:.2}) to place next trade (${:.2})",
                    engine_state.cached_balance, engine_state.current_trade_amount
                );
                tracing::warn!(
                    "User {} balance ${:.2} < trade amount ${:.2} with no open trade, auto-stopping",
                    user_id, engine_state.cached_balance, engine_state.current_trade_amount
                );
                let _ = crate::bot_config::service::set_active(&pool, &state.bot_configs, &user_id, false).await;
                let _ = state.balance_tx.send(serde_json::json!({
                    "type": "engine_stopped",
                    "user_id": user_id,
                    "reason": reason,
                    "balance": engine_state.cached_balance,
                }).to_string());
                break;
            }

            // Safety: min balance floor
            if let Some(min_bal) = config.min_balance_threshold {
                if engine_state.cached_balance < min_bal {
                    let reason = format!(
                        "Balance (${:.2}) fell below minimum threshold (${:.2})",
                        engine_state.cached_balance, min_bal
                    );
                    tracing::warn!(
                        "User {} balance ${:.2} below floor ${:.2} with no open trade, auto-stopping",
                        user_id, engine_state.cached_balance, min_bal
                    );
                    let _ = crate::bot_config::service::set_active(&pool, &state.bot_configs, &user_id, false).await;
                    let _ = state.balance_tx.send(serde_json::json!({
                        "type": "engine_stopped",
                        "user_id": user_id,
                        "reason": reason,
                        "balance": engine_state.cached_balance,
                    }).to_string());
                    break;
                }
            }
        }

        // Safety: daily loss limit
        if let Some(limit) = config.daily_loss_limit {
            if engine_state.daily_pnl <= -limit {
                let reason = format!(
                    "Daily loss limit reached (${:.2} lost, limit ${:.2})",
                    engine_state.daily_pnl.abs(), limit
                );
                tracing::warn!(
                    "User {} daily PnL ${:.2} hit loss limit -${:.2}, auto-stopping",
                    user_id, engine_state.daily_pnl, limit
                );
                let _ = crate::bot_config::service::set_active(&pool, &state.bot_configs, &user_id, false).await;
                let _ = state.balance_tx.send(serde_json::json!({
                    "type": "engine_stopped",
                    "user_id": user_id,
                    "reason": reason,
                    "balance": engine_state.cached_balance,
                }).to_string());
                break;
            }
        }

        // Direction exhaustion check
        if config.target_direction.is_some()
            && engine_state.direction_markets_traded >= config.target_markets_count
        {
            match config.direction_after_count.as_str() {
                "stop" => {
                    let reason = format!(
                        "Completed {} {} market(s) — direction target reached",
                        config.target_markets_count,
                        config.target_direction.as_deref().unwrap_or("targeted")
                    );
                    tracing::info!("User {}: direction target exhausted, stopping", user_id);
                    let _ = crate::bot_config::service::set_active(&pool, &state.bot_configs, &user_id, false).await;
                    let _ = state.balance_tx.send(serde_json::json!({
                        "type": "engine_stopped",
                        "user_id": user_id,
                        "reason": reason,
                        "balance": engine_state.cached_balance,
                    }).to_string());
                    break;
                }
                "flip" | "any" => {
                    // The direction logic in find_market handles this — we just note it
                }
                _ => {}
            }
        }

        // ── Process resolved trades FIRST (frees position slots, updates streaks) ──
        process_resolved_trades(&state, &pool, &user_id, &privy_user_id, &config, &mut engine_state).await;

        // ── Find the current active market for this user's timeframe ──
        let market = match find_active_market(&state, &pool, &config).await {
            Some(m) => m,
            None => continue, // no active market right now
        };

        // ── Check max open positions ──
        let open_count = count_open_trades(&pool, &user_id).await.unwrap_or(0);
        if open_count >= config.max_open_positions as i64 {
            continue; // at capacity
        }

        // ── Skip if already traded this market (unless allow_retrade is enabled) ──
        if engine_state.traded_market_ids.contains(&market.id) && !config.allow_retrade {
            continue;
        }

        // ── Determine trade side ──
        let side = determine_trade_side(&config, &engine_state, &market, &state);

        // ── Get live price for the chosen side ──
        let (clob_token_id, live_price) = match get_side_price(&state, &market, &side) {
            Some(v) => v,
            None => continue, // no price available
        };

        // ── Check entry timing condition ──
        if !check_entry_timing(&market, &config) {
            continue;
        }

        // ── Check entry price condition ──
        if !check_entry_price(live_price, &config) {
            continue;
        }

        // ── Compute trade amount (with max cap) ──
        let trade_amount = compute_trade_amount(&config, &engine_state);

        // ── EXECUTE BUY ──
        tracing::info!(
            "User {} BUY: market={} side={} amount=${:.2} price={:.4}",
            user_id,
            market.slug,
            side,
            trade_amount,
            live_price
        );

        let outcome = crate::mevu_client::outcome_from_side(&side);

        match executor::execute_buy(
            &state.http_client,
            &privy_user_id,
            &market.id,
            &market.title,
            &clob_token_id,
            outcome,
            trade_amount,
            live_price,
        )
        .await
        {
            Ok(fill) => {
                let trade = BotTrade {
                    id: uuid_v4(),
                    user_id: user_id.clone(),
                    market_id: market.id.clone(),
                    market_slug: market.slug.clone(),
                    market_title: market.title.clone(),
                    timeframe: config.trade_timeframe.clone(),
                    side: side.clone(),
                    buy_amount: fill.fill_amount,
                    entry_price: fill.fill_price,
                    exit_price: None,
                    payout: None,
                    pnl: None,
                    status: "open".to_string(),
                    clob_token_id: clob_token_id.clone(),
                    error: None,
                    created_at: chrono::Utc::now().to_rfc3339(),
                    updated_at: chrono::Utc::now().to_rfc3339(),
                };

                if let Err(e) = save_trade(&pool, &trade).await {
                    tracing::error!("Failed to save trade: {}", e);
                }

                engine_state.traded_market_ids.push(market.id.clone());
                engine_state.cached_balance -= trade_amount;
                // Broadcast updated balance to SSE clients
                let _ = state.balance_tx.send(serde_json::json!({
                    "type": "trade_update",
                    "user_id": user_id,
                    "privy_user_id": privy_user_id,
                    "balance": engine_state.cached_balance,
                }).to_string());
                // Count this market toward direction targeting limit at BUY time
                if config.target_direction.is_some() {
                    engine_state.direction_markets_traded += 1;
                }

                tracing::info!(
                    "User {} BUY OK: market={} side={} filled=${:.2}@{:.4}",
                    user_id, market.slug, side, fill.fill_amount, fill.fill_price
                );
            }
            Err(e) => {
                tracing::error!("User {} BUY FAILED: {}", user_id, e);
                // Record failed trade
                let trade = BotTrade {
                    id: uuid_v4(),
                    user_id: user_id.clone(),
                    market_id: market.id.clone(),
                    market_slug: market.slug.clone(),
                    market_title: market.title.clone(),
                    timeframe: config.trade_timeframe.clone(),
                    side: side.clone(),
                    buy_amount: trade_amount,
                    entry_price: live_price,
                    exit_price: None,
                    payout: None,
                    pnl: None,
                    status: "error".to_string(),
                    clob_token_id: clob_token_id.clone(),
                    error: Some(e),
                    created_at: chrono::Utc::now().to_rfc3339(),
                    updated_at: chrono::Utc::now().to_rfc3339(),
                };
                let _ = save_trade(&pool, &trade).await;
            }
        }
    }
}

// ─── Market Discovery ──────────────────────────────────────────────────────

/// Active market info needed by the engine
#[allow(dead_code)]
struct ActiveMarket {
    id: String,
    slug: String,
    title: String,
    start_time: Option<chrono::DateTime<chrono::Utc>>,
    end_date: Option<chrono::DateTime<chrono::Utc>>,
    up_clob_token_id: Option<String>,
    down_clob_token_id: Option<String>,
}

/// Find the current active (not resolved) market for the user's configured timeframe.
async fn find_active_market(
    state: &Arc<AppState>,
    pool: &PgPool,
    config: &BotConfig,
) -> Option<ActiveMarket> {
    let now = chrono::Utc::now();
    let today = now.format("%Y-%m-%d").to_string();
    let date_pattern = format!("{}%", today);

    let rows = sqlx::query(
        "SELECT id, slug, title, start_time, end_date, up_clob_token_id, down_clob_token_id
         FROM crypto_markets
         WHERE asset = 'bitcoin'
           AND timeframe = $1
           AND (end_date LIKE $2 OR start_time LIKE $2)
         ORDER BY start_time ASC NULLS LAST"
    )
    .bind(&config.trade_timeframe)
    .bind(&date_pattern)
    .fetch_all(pool)
    .await
    .ok()?;

    for row in &rows {
        use sqlx::Row;
        let id: String = row.try_get("id").unwrap_or_default();
        let slug: String = row.try_get("slug").unwrap_or_default();
        let title: String = row.try_get("title").unwrap_or_default();
        let start_time_str: Option<String> = row.try_get("start_time").ok().flatten();
        let end_date_str: Option<String> = row.try_get("end_date").ok().flatten();
        let up_clob: Option<String> = row.try_get("up_clob_token_id").ok().flatten();
        let down_clob: Option<String> = row.try_get("down_clob_token_id").ok().flatten();

        let start = start_time_str.as_deref().and_then(parse_dt);
        let end = end_date_str.as_deref().and_then(parse_dt);

        // Market must be active (started, not yet ended)
        let is_active = match (start, end) {
            (Some(s), Some(e)) => now >= s && now < e,
            _ => false,
        };

        if is_active {
            return Some(ActiveMarket {
                id,
                slug,
                title,
                start_time: start,
                end_date: end,
                up_clob_token_id: up_clob,
                down_clob_token_id: down_clob,
            });
        }
    }

    None
}

// ─── Condition Checks ──────────────────────────────────────────────────────

/// Determine which side to trade based on config direction targeting.
/// When no direction is forced, picks the cheaper side (lower price = higher upside).
fn determine_trade_side(
    config: &BotConfig,
    engine_state: &UserEngineState,
    market: &ActiveMarket,
    state: &Arc<AppState>,
) -> String {
    if let Some(ref dir) = config.target_direction {
        // Check if direction target has been exhausted
        if engine_state.direction_markets_traded >= config.target_markets_count {
            match config.direction_after_count.as_str() {
                "flip" => {
                    return if dir == "up" { "down".to_string() } else { "up".to_string() };
                }
                "any" => {
                    // Revert to auto-pick: fall through to price-based selection below
                }
                // "stop" is handled earlier in the main loop (breaks engine)
                _ => return dir.clone(),
            }
        } else {
            return dir.clone();
        }
    }

    // Auto-pick: prefer the side that meets the entry price condition.
    // If both meet it, pick the cheaper one. If neither meets it, still pick
    // the cheaper one (the entry price check later will skip the trade).
    let up_price = market.up_clob_token_id.as_ref()
        .and_then(|id| state.price_cache.get(id).map(|p| p.best_ask));
    let down_price = market.down_clob_token_id.as_ref()
        .and_then(|id| state.price_cache.get(id).map(|p| p.best_ask));

    let meets_condition = |price: f64| -> bool {
        let base = match config.entry_condition.as_str() {
            "lte" => price <= config.entry_price_threshold,
            "gte" => price >= config.entry_price_threshold,
            "eq" => price >= config.entry_price_threshold,
            _ => false,
        };
        if !base { return false; }
        if let Some(max) = config.entry_price_max {
            return price <= max;
        }
        true
    };

    let up_meets = up_price.map(|p| meets_condition(p)).unwrap_or(false);
    let down_meets = down_price.map(|p| meets_condition(p)).unwrap_or(false);

    match (up_meets, down_meets) {
        (true, true) => {
            // Both qualify — pick the cheaper one (higher payout potential)
            match (up_price, down_price) {
                (Some(up), Some(down)) => if up <= down { "up".to_string() } else { "down".to_string() },
                _ => "up".to_string(),
            }
        }
        (true, false) => "up".to_string(),
        (false, true) => "down".to_string(),
        (false, false) => {
            // Neither qualifies — pick cheaper side (trade will be skipped by entry check)
            match (up_price, down_price) {
                (Some(up), Some(down)) => if up <= down { "up".to_string() } else { "down".to_string() },
                (Some(_), None) => "up".to_string(),
                (None, Some(_)) => "down".to_string(),
                (None, None) => "up".to_string(),
            }
        }
    }
}

/// Get the CLOB token ID and live price for the chosen side.
fn get_side_price(state: &Arc<AppState>, market: &ActiveMarket, side: &str) -> Option<(String, f64)> {
    let token_id = match side {
        "up" => market.up_clob_token_id.as_ref()?,
        "down" => market.down_clob_token_id.as_ref()?,
        _ => return None,
    };

    let price = state.price_cache.get(token_id).map(|p| p.best_ask)?;

    // Sanity: price must be in valid range
    if price <= 0.0 || price >= 1.0 {
        return None;
    }

    Some((token_id.clone(), price))
}

/// Check if we're within the entry timing window (seconds before market close).
fn check_entry_timing(market: &ActiveMarket, config: &BotConfig) -> bool {
    // 0 = buy anytime conditions are met
    if config.entry_time_before_close == 0 {
        return true;
    }

    let end = match market.end_date {
        Some(e) => e,
        None => return false,
    };

    let now = chrono::Utc::now();
    let seconds_until_close = (end - now).num_seconds();

    // Must be within the timing window
    seconds_until_close >= 0 && seconds_until_close <= config.entry_time_before_close as i64
}

/// Check if the live price meets the entry price condition.
/// When `entry_price_max` is set, also enforces an upper bound to prevent
/// buying on sudden price spikes past the intended range.
fn check_entry_price(live_price: f64, config: &BotConfig) -> bool {
    let base_check = match config.entry_condition.as_str() {
        "lte" => live_price <= config.entry_price_threshold,
        "gte" => live_price >= config.entry_price_threshold,
        "eq" => live_price >= config.entry_price_threshold,
        _ => false,
    };

    if !base_check {
        return false;
    }

    // If a max price ceiling is set, enforce it
    if let Some(max) = config.entry_price_max {
        return live_price <= max;
    }

    true
}

/// Compute the trade amount respecting streak adjustments and max cap.
fn compute_trade_amount(config: &BotConfig, engine_state: &UserEngineState) -> f64 {
    let mut amount = engine_state.current_trade_amount;

    // Cap at max_trade_amount if set
    if let Some(max) = config.max_trade_amount {
        if amount > max {
            amount = max;
        }
    }

    // Floor at a minimum of $0.01
    if amount < 0.01 {
        amount = 0.01;
    }

    amount
}

// ─── Exit / Resolution Processing ──────────────────────────────────────────

/// Process all open trades: check for sell_at exit and resolved markets.
async fn process_resolved_trades(
    state: &Arc<AppState>,
    pool: &PgPool,
    user_id: &str,
    privy_user_id: &str,
    config: &BotConfig,
    engine_state: &mut UserEngineState,
) {
    let open_trades = match load_open_trades(pool, user_id).await {
        Ok(trades) => trades,
        Err(_) => return,
    };

    for trade in open_trades {
        // Check if market has resolved
        let resolved = check_market_resolved(pool, &trade.market_id).await;

        if resolved {
            // Market resolved: check result and record win/loss
            let result = get_market_result(pool, &trade.market_id).await;

            let won = match &result {
                Some(r) => r == &trade.side,
                None => false, // unknown result, assume loss
            };

            if won {
                // Try to redeem via MEVU using the market title + side → outcome mapping.
                // Positions may not be redeemable for 30-60s after market resolution,
                // so retry up to 4 times with increasing delays (10s, 15s, 20s, 25s).
                //
                // Before each retry, check if the user still holds the position. If it's gone,
                // the user already redeemed or sold it manually — stop retrying.
                let mut redeemed = false;
                let mut manually_handled = false;
                let retry_delays = [10u64, 15, 20, 25, 30];
                for (attempt, delay_secs) in retry_delays.iter().enumerate() {
                    if *delay_secs > 0 {
                        tracing::info!(
                            "User {} REDEEM retry {}/{} for market={} in {}s",
                            user_id, attempt, retry_delays.len() - 1, trade.market_slug, delay_secs
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(*delay_secs)).await;

                        // Check if user still holds this position before retrying.
                        // Match on `asset` field which is the clob_token_id — a unique stable identifier
                        // for the specific outcome token, avoiding title/outcome formatting mismatches.
                        if let Ok(positions) = crate::mevu_client::fetch_positions(&state.http_client, privy_user_id).await {
                            let still_held = positions.iter().any(|p| {
                                p.asset.as_deref() == Some(&trade.clob_token_id)
                            });
                            if !still_held && !positions.is_empty() {
                                // User has positions but not this one — sold/redeemed manually.
                                // If positions list is empty, the API may have returned stale data, so keep retrying.
                                tracing::info!(
                                    "User {} position for market={} (asset={}) no longer held (sold/redeemed manually), skipping retries. User positions: {:?}",
                                    user_id, trade.market_slug, trade.clob_token_id,
                                    positions.iter().map(|p| format!("asset={}|title={}|outcome={}", p.asset.as_deref().unwrap_or("?"), p.title.as_deref().unwrap_or("?"), p.outcome.as_deref().unwrap_or("?"))).collect::<Vec<_>>()
                                );
                                manually_handled = true;
                                break;
                            }
                        }
                    }
                    match executor::execute_redeem(
                        &state.http_client,
                        privy_user_id,
                        &trade.clob_token_id,
                    )
                    .await
                    {
                        Ok(fill) => {
                            let pnl = fill.payout - trade.buy_amount;
                            update_trade_result(pool, &trade.id, "won", Some(1.0), Some(fill.payout), Some(pnl)).await;
                            engine_state.cached_balance += fill.payout;
                            engine_state.daily_pnl += pnl;
                            let _ = state.balance_tx.send(serde_json::json!({
                                "type": "trade_update", "user_id": user_id,
                                "privy_user_id": privy_user_id, "balance": engine_state.cached_balance,
                            }).to_string());
                            apply_win_streak(config, engine_state);
                            tracing::info!("User {} WIN: market={} pnl=${:.2} daily_pnl=${:.2}", user_id, trade.market_slug, pnl, engine_state.daily_pnl);
                            redeemed = true;
                            break;
                        }
                        Err(e) => {
                            tracing::warn!("User {} REDEEM attempt {} failed: {}", user_id, attempt + 1, e);
                        }
                    }
                }
                if manually_handled {
                    // User redeemed/sold outside the bot — mark as won, payout already collected externally
                    update_trade_result(pool, &trade.id, "won", Some(1.0), None, None).await;
                    apply_win_streak(config, engine_state);
                    // Refresh balance since user may have received payout
                    refresh_balance(&state.http_client, privy_user_id, engine_state).await;
                } else if !redeemed {
                    tracing::error!("User {} REDEEM FAILED after {} attempts for market={}", user_id, retry_delays.len(), trade.market_slug);
                    // Mark as won but payout not collected — can be claimed manually later
                    update_trade_result(pool, &trade.id, "won", Some(1.0), None, None).await;
                    apply_win_streak(config, engine_state);
                }
            } else {
                // Loss
                let pnl = -trade.buy_amount;
                update_trade_result(pool, &trade.id, "lost", Some(0.0), Some(0.0), Some(pnl)).await;
                engine_state.daily_pnl += pnl;
                apply_loss_streak(config, engine_state);
                tracing::info!("User {} LOSS: market={} pnl=${:.2} daily_pnl=${:.2}", user_id, trade.market_slug, pnl, engine_state.daily_pnl);
            }

            continue;
        }

        // Check sell_at exit condition
        if config.exit_strategy == "sell_at" {
            if let Some(exit_threshold) = config.exit_price_threshold {
                let current_price = state.price_cache.get(&trade.clob_token_id).map(|p| p.best_ask);
                if let Some(price) = current_price {
                    if price >= exit_threshold {
                        // Resolve share count to sell:
                        // - If sell_amount (USDC) configured → convert to shares at current price
                        // - Otherwise → fetch the actual position size (sell all shares)
                        let size_shares = if let Some(usdc_amount) = config.sell_amount {
                            if price > 0.0 { usdc_amount / price } else { 0.0 }
                        } else {
                            // Fetch live position to get the real share count
                            match crate::mevu_client::fetch_positions(&state.http_client, privy_user_id).await {
                                Ok(positions) => {
                                    positions.iter()
                                        .find(|p| p.asset.as_deref() == Some(&trade.clob_token_id))
                                        .and_then(|p| p.size.as_deref())
                                        .and_then(|s| s.parse::<f64>().ok())
                                        .unwrap_or_else(|| {
                                            tracing::warn!("User {} could not find position size for {}, falling back to buy_amount/price", user_id, trade.clob_token_id);
                                            if price > 0.0 { trade.buy_amount / price } else { 0.0 }
                                        })
                                }
                                Err(e) => {
                                    tracing::warn!("User {} failed to fetch position size for sell: {}, falling back", user_id, e);
                                    if price > 0.0 { trade.buy_amount / price } else { 0.0 }
                                }
                            }
                        };
                        let outcome = crate::mevu_client::outcome_from_side(&trade.side);
                        match executor::execute_sell(
                            &state.http_client,
                            privy_user_id,
                            &trade.market_id,
                            &trade.market_title,
                            &trade.clob_token_id,
                            outcome,
                            size_shares,
                            price,
                        ).await {
                            Ok(fill) => {
                                let pnl = fill.payout - trade.buy_amount;
                                update_trade_result(pool, &trade.id, "sold", Some(fill.fill_price), Some(fill.payout), Some(pnl)).await;
                                engine_state.cached_balance += fill.payout;
                                engine_state.daily_pnl += pnl;
                                let _ = state.balance_tx.send(serde_json::json!({
                                    "type": "trade_update", "user_id": user_id,
                                    "privy_user_id": privy_user_id, "balance": engine_state.cached_balance,
                                }).to_string());
                                if pnl > 0.0 {
                                    apply_win_streak(config, engine_state);
                                } else {
                                    apply_loss_streak(config, engine_state);
                                }
                                tracing::info!("User {} SOLD: market={} pnl=${:.2} daily_pnl=${:.2}", user_id, trade.market_slug, pnl, engine_state.daily_pnl);
                            }
                            Err(e) => {
                                tracing::error!("User {} SELL FAILED: {}", user_id, e);
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Apply win streak adjustment to trade amount.
fn apply_win_streak(config: &BotConfig, state: &mut UserEngineState) {
    state.win_streak += 1;
    state.loss_streak = 0;

    // Check if win streak limit triggers a reset
    if config.win_streak_limit > 0 && state.win_streak >= config.win_streak_limit {
        tracing::info!("Win streak limit {} reached, resetting to base ${:.2}", config.win_streak_limit, config.base_amount);
        state.current_trade_amount = config.base_amount;
        state.win_streak = 0;
        return;
    }

    match config.win_action.as_str() {
        "double" => {
            state.current_trade_amount *= 2.0;
        }
        "half" => {
            state.current_trade_amount /= 2.0;
        }
        _ => {} // "none" — keep same
    }
}

/// Apply loss streak adjustment to trade amount.
fn apply_loss_streak(config: &BotConfig, state: &mut UserEngineState) {
    state.loss_streak += 1;
    state.win_streak = 0;

    // Check if loss streak limit triggers a reset
    if config.loss_streak_limit > 0 && state.loss_streak >= config.loss_streak_limit {
        tracing::info!("Loss streak limit {} reached, resetting to base ${:.2}", config.loss_streak_limit, config.base_amount);
        state.current_trade_amount = config.base_amount;
        state.loss_streak = 0;
        return;
    }

    match config.loss_action.as_str() {
        "double" => {
            state.current_trade_amount *= 2.0;
        }
        "half" => {
            state.current_trade_amount /= 2.0;
        }
        _ => {} // "none" — keep same
    }
}

// ─── DB Operations ─────────────────────────────────────────────────────────

/// Save a new trade record to the database.
async fn save_trade(pool: &PgPool, trade: &BotTrade) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO bot_trades (
            id, user_id, market_id, market_slug, market_title, timeframe,
            side, buy_amount, entry_price, exit_price, payout, pnl,
            status, clob_token_id, error, created_at, updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,NOW(),NOW())"
    )
    .bind(&trade.id)
    .bind(&trade.user_id)
    .bind(&trade.market_id)
    .bind(&trade.market_slug)
    .bind(&trade.market_title)
    .bind(&trade.timeframe)
    .bind(&trade.side)
    .bind(trade.buy_amount)
    .bind(trade.entry_price)
    .bind(trade.exit_price)
    .bind(trade.payout)
    .bind(trade.pnl)
    .bind(&trade.status)
    .bind(&trade.clob_token_id)
    .bind(&trade.error)
    .execute(pool)
    .await?;
    Ok(())
}

/// Update a trade's result after resolution or sell.
async fn update_trade_result(
    pool: &PgPool,
    trade_id: &str,
    status: &str,
    exit_price: Option<f64>,
    payout: Option<f64>,
    pnl: Option<f64>,
) {
    let _ = sqlx::query(
        "UPDATE bot_trades SET status=$1, exit_price=$2, payout=$3, pnl=$4, updated_at=NOW() WHERE id=$5"
    )
    .bind(status)
    .bind(exit_price)
    .bind(payout)
    .bind(pnl)
    .bind(trade_id)
    .execute(pool)
    .await;
}

/// Load all open trades for a user.
async fn load_open_trades(pool: &PgPool, user_id: &str) -> Result<Vec<BotTrade>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT * FROM bot_trades WHERE user_id = $1 AND status = 'open' ORDER BY created_at ASC"
    )
    .bind(user_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(row_to_trade).collect())
}

/// Load market IDs that have a successful (non-error) trade today.
/// This prevents re-entering a market that was already traded, while allowing
/// retry after failed attempts (status = 'error').
async fn load_traded_market_ids_today(pool: &PgPool, user_id: &str) -> Result<Vec<String>, sqlx::Error> {
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let date_pattern = format!("{}%", today);
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT market_id FROM bot_trades WHERE user_id = $1 AND status != 'error' AND created_at::TEXT LIKE $2"
    )
    .bind(user_id)
    .bind(&date_pattern)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Returns true if the user has an open trade whose market has NOT yet expired.
///
/// Market end time is parsed from the slug (e.g. `btc-updown-15m-1773777600` → unix 1773777600).
/// A 5-minute grace window is added so the redeem retry loop can run before we unblock balance checks.
/// Trades past their market end + grace are stale (manually sold or in retry) and don't block.
async fn has_active_open_trade(pool: &PgPool, user_id: &str) -> bool {
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT market_slug FROM bot_trades WHERE user_id = $1 AND status = 'open'"
    )
    .bind(user_id)
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    if rows.is_empty() {
        return false;
    }

    let now = chrono::Utc::now().timestamp();
    const GRACE_SECS: i64 = 300; // 5 minutes after market end before considered stale

    for slug in &rows {
        // Parse trailing unix timestamp from slug, e.g. "btc-updown-15m-1773777600" → 1773777600
        if let Some(end_ts) = slug.rsplit('-').next().and_then(|s| s.parse::<i64>().ok()) {
            if now < end_ts + GRACE_SECS {
                return true; // this trade's market is still live
            }
            // else: market has ended + grace elapsed — treat as stale, skip
        } else {
            // Can't parse slug timestamp — conservatively treat as active
            return true;
        }
    }

    false // all open trades are past their market end + grace
}

/// Count open trades for a user.
async fn count_open_trades(pool: &PgPool, user_id: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM bot_trades WHERE user_id = $1 AND status = 'open'"
    )
    .bind(user_id)
    .fetch_one(pool)
    .await
}

/// Check if a market has resolved (end_date has passed).
async fn check_market_resolved(pool: &PgPool, market_id: &str) -> bool {
    let row = sqlx::query(
        "SELECT end_date FROM crypto_markets WHERE id = $1"
    )
    .bind(market_id)
    .fetch_optional(pool)
    .await;

    match row {
        Ok(Some(row)) => {
            use sqlx::Row;
            let end_str: Option<String> = row.try_get("end_date").ok().flatten();
            let end = end_str.as_deref().and_then(parse_dt);
            match end {
                Some(e) => chrono::Utc::now() >= e,
                None => false,
            }
        }
        _ => false,
    }
}

/// Get the resolved result of a market: "up" or "down" based on opening vs closing BTC price.
async fn get_market_result(pool: &PgPool, market_id: &str) -> Option<String> {
    let row = sqlx::query(
        "SELECT opening_btc_price, closing_btc_price, up_price, down_price FROM crypto_markets WHERE id = $1"
    )
    .bind(market_id)
    .fetch_optional(pool)
    .await
    .ok()??;

    use sqlx::Row;
    let opening: Option<f64> = row.try_get("opening_btc_price").ok().flatten();
    let closing: Option<f64> = row.try_get("closing_btc_price").ok().flatten();

    // Primary: compare BTC open vs close
    if let (Some(open), Some(close)) = (opening, closing) {
        if close > open { return Some("up".to_string()); }
        else if close < open { return Some("down".to_string()); }
    }

    // Fallback: CLOB odds comparison
    let up: Option<f64> = row.try_get("up_price").ok().flatten();
    let down: Option<f64> = row.try_get("down_price").ok().flatten();
    let up_v = up.unwrap_or(0.0);
    let down_v = down.unwrap_or(0.0);
    if up_v > down_v { Some("up".to_string()) }
    else if down_v > up_v { Some("down".to_string()) }
    else { None }
}

/// Map a PgRow to BotTrade.
fn row_to_trade(row: &sqlx::postgres::PgRow) -> BotTrade {
    use sqlx::Row;
    BotTrade {
        id: row.try_get("id").unwrap_or_default(),
        user_id: row.try_get("user_id").unwrap_or_default(),
        market_id: row.try_get("market_id").unwrap_or_default(),
        market_slug: row.try_get("market_slug").unwrap_or_default(),
        market_title: row.try_get("market_title").unwrap_or_default(),
        timeframe: row.try_get("timeframe").unwrap_or_default(),
        side: row.try_get("side").unwrap_or_default(),
        buy_amount: row.try_get("buy_amount").unwrap_or(0.0),
        entry_price: row.try_get("entry_price").unwrap_or(0.0),
        exit_price: row.try_get("exit_price").ok().flatten(),
        payout: row.try_get("payout").ok().flatten(),
        pnl: row.try_get("pnl").ok().flatten(),
        status: row.try_get("status").unwrap_or_default(),
        clob_token_id: row.try_get("clob_token_id").unwrap_or_default(),
        error: row.try_get("error").ok().flatten(),
        created_at: row.try_get::<chrono::DateTime<chrono::Utc>, _>("created_at")
            .map(|dt| dt.to_rfc3339()).unwrap_or_default(),
        updated_at: row.try_get::<chrono::DateTime<chrono::Utc>, _>("updated_at")
            .map(|dt| dt.to_rfc3339()).unwrap_or_default(),
    }
}

// ─── Helpers ───────────────────────────────────────────────────────────────

/// After reconstructing streak counts, replay the streak adjustments to compute
/// the correct current_trade_amount (e.g., if we had 3 consecutive losses with "double",
/// the trade amount should be base * 2^3).
fn replay_streak_amount(config: &BotConfig, state: &mut UserEngineState) {
    state.current_trade_amount = config.base_amount;

    if state.win_streak > 0 {
        let effective_streak = if config.win_streak_limit > 0 {
            state.win_streak % config.win_streak_limit
        } else {
            state.win_streak
        };
        for _ in 0..effective_streak {
            match config.win_action.as_str() {
                "double" => state.current_trade_amount *= 2.0,
                "half" => state.current_trade_amount /= 2.0,
                _ => {}
            }
        }
    } else if state.loss_streak > 0 {
        let effective_streak = if config.loss_streak_limit > 0 {
            state.loss_streak % config.loss_streak_limit
        } else {
            state.loss_streak
        };
        for _ in 0..effective_streak {
            match config.loss_action.as_str() {
                "double" => state.current_trade_amount *= 2.0,
                "half" => state.current_trade_amount /= 2.0,
                _ => {}
            }
        }
    }

    // Apply max cap
    if let Some(max) = config.max_trade_amount {
        if state.current_trade_amount > max {
            state.current_trade_amount = max;
        }
    }

    if state.current_trade_amount != config.base_amount {
        tracing::info!(
            "Replayed streak amount: base=${:.2} → current=${:.2}",
            config.base_amount, state.current_trade_amount
        );
    }
}

/// Reconstruct streak state from today's completed trades (for restart recovery).
/// Walks trades in chronological order and computes the current streak.
async fn reconstruct_streak_state(pool: &PgPool, user_id: &str, engine_state: &mut UserEngineState) {
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let date_pattern = format!("{}%", today);

    let rows = sqlx::query(
        "SELECT status, pnl FROM bot_trades
         WHERE user_id = $1 AND created_at::TEXT LIKE $2 AND status IN ('won', 'lost', 'sold')
         ORDER BY created_at ASC"
    )
    .bind(user_id)
    .bind(&date_pattern)
    .fetch_all(pool)
    .await;

    if let Ok(rows) = rows {
        for row in &rows {
            use sqlx::Row;
            let status: String = row.try_get("status").unwrap_or_default();
            let pnl: Option<f64> = row.try_get("pnl").ok().flatten();

            // Accumulate daily PnL from resolved trades
            if let Some(p) = pnl {
                engine_state.daily_pnl += p;
            }

            let is_win = match status.as_str() {
                "won" => true,
                "lost" => false,
                "sold" => pnl.unwrap_or(0.0) > 0.0,
                _ => continue,
            };

            if is_win {
                engine_state.win_streak += 1;
                engine_state.loss_streak = 0;
            } else {
                engine_state.loss_streak += 1;
                engine_state.win_streak = 0;
            }
        }

        // Count direction-targeted trades placed today
        let dir_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM bot_trades
             WHERE user_id = $1 AND created_at::TEXT LIKE $2 AND status != 'error'"
        )
        .bind(user_id)
        .bind(&date_pattern)
        .fetch_one(pool)
        .await
        .unwrap_or(0);

        engine_state.direction_markets_traded = dir_count as i32;

        if engine_state.win_streak > 0 || engine_state.loss_streak > 0 {
            tracing::info!(
                "Reconstructed streak state for user {}: win_streak={} loss_streak={} dir_traded={}",
                user_id, engine_state.win_streak, engine_state.loss_streak, engine_state.direction_markets_traded
            );
        }
    }
}

/// Refresh cached balance from MEVU.
async fn refresh_balance(http_client: &reqwest::Client, privy_user_id: &str, state: &mut UserEngineState) {
    match crate::mevu_client::fetch_balance(http_client, privy_user_id).await {
        Ok(bal) => {
            let human = bal.human_balance
                .as_ref()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            state.cached_balance = human;
            state.last_balance_refresh = Some(chrono::Utc::now());
        }
        Err(e) => {
            tracing::warn!("Balance refresh failed: {}", e);
        }
    }
}

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

fn uuid_v4() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let nanos = ts.as_nanos();
    // Simple pseudo-UUID: timestamp + random bits
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        (nanos >> 64) as u32,
        (nanos >> 48) as u16 & 0xFFFF,
        (nanos >> 36) as u16 & 0x0FFF,
        ((nanos >> 24) as u16 & 0x3FFF) | 0x8000,
        nanos as u64 & 0xFFFFFFFFFFFF,
    )
}
