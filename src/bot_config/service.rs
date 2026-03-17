use super::types::{BotConfig, UpdateBotConfigRequest};
use dashmap::DashMap;
use sqlx::{PgPool, Row};
use std::sync::Arc;

/// In-memory config cache: user_id → BotConfig
pub type ConfigCache = Arc<DashMap<String, BotConfig>>;

/// Get config from cache (fast path) or DB (cold path).
pub async fn get_config(
    pool: &PgPool,
    cache: &ConfigCache,
    user_id: &str,
) -> Result<Option<BotConfig>, sqlx::Error> {
    // Fast path: check in-memory cache
    if let Some(entry) = cache.get(user_id) {
        return Ok(Some(entry.value().clone()));
    }

    // Cold path: query DB
    let row = sqlx::query(
        "SELECT * FROM bot_configs WHERE user_id = $1",
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await?;

    match row {
        Some(row) => {
            let cfg = row_to_config(&row);
            cache.insert(user_id.to_string(), cfg.clone());
            Ok(Some(cfg))
        }
        None => Ok(None),
    }
}

/// Create or update a config. Uses INSERT ON CONFLICT for upsert.
/// Merges provided fields with existing values (COALESCE pattern).
pub async fn upsert_config(
    pool: &PgPool,
    cache: &ConfigCache,
    user_id: &str,
    req: &UpdateBotConfigRequest,
) -> Result<BotConfig, sqlx::Error> {
    // Load current config to merge with (or use defaults)
    let current = get_config(pool, cache, user_id).await?.unwrap_or_else(|| default_config(user_id));

    let base_amount = req.base_amount.unwrap_or(current.base_amount);
    let sell_amount = match &req.sell_amount {
        Some(v) => Some(*v),
        None => current.sell_amount,
    };
    let max_trade_amount = match &req.max_trade_amount {
        Some(v) => *v,
        None => current.max_trade_amount,
    };
    let max_open_positions = req.max_open_positions.unwrap_or(current.max_open_positions);
    let target_direction = match &req.target_direction {
        Some(v) => v.clone(),
        None => current.target_direction,
    };
    let target_markets_count = req.target_markets_count.unwrap_or(current.target_markets_count);
    let direction_after_count = req.direction_after_count.clone().unwrap_or(current.direction_after_count);
    let trade_timeframe = req.trade_timeframe.clone().unwrap_or(current.trade_timeframe);
    let entry_time_before_close = req.entry_time_before_close.unwrap_or(current.entry_time_before_close);
    let entry_condition = req.entry_condition.clone().unwrap_or(current.entry_condition);
    let entry_price_threshold = req.entry_price_threshold.unwrap_or(current.entry_price_threshold);
    let entry_price_max = match &req.entry_price_max {
        Some(v) => *v,
        None => current.entry_price_max,
    };
    let exit_strategy = req.exit_strategy.clone().unwrap_or(current.exit_strategy);
    let exit_price_threshold = match &req.exit_price_threshold {
        Some(v) => *v,
        None => current.exit_price_threshold,
    };
    let loss_action = req.loss_action.clone().unwrap_or(current.loss_action);
    let loss_streak_limit = req.loss_streak_limit.unwrap_or(current.loss_streak_limit);
    let win_action = req.win_action.clone().unwrap_or(current.win_action);
    let win_streak_limit = req.win_streak_limit.unwrap_or(current.win_streak_limit);
    let auto_stop = req.auto_stop_on_low_balance.unwrap_or(current.auto_stop_on_low_balance);
    let min_balance = match &req.min_balance_threshold {
        Some(v) => *v,
        None => current.min_balance_threshold,
    };
    let daily_loss_limit = match &req.daily_loss_limit {
        Some(v) => *v,
        None => current.daily_loss_limit,
    };
    let allow_retrade = req.allow_retrade.unwrap_or(current.allow_retrade);

    let row = sqlx::query(
        r#"
        INSERT INTO bot_configs (
            user_id, base_amount, sell_amount,
            max_trade_amount, max_open_positions,
            target_direction, target_markets_count, direction_after_count,
            trade_timeframe, entry_time_before_close,
            entry_condition, entry_price_threshold, entry_price_max,
            exit_strategy, exit_price_threshold,
            loss_action, loss_streak_limit,
            win_action, win_streak_limit,
            auto_stop_on_low_balance, min_balance_threshold,
            daily_loss_limit,
            allow_retrade,
            is_active, created_at, updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24, NOW(), NOW())
        ON CONFLICT (user_id) DO UPDATE SET
            base_amount = EXCLUDED.base_amount,
            sell_amount = EXCLUDED.sell_amount,
            max_trade_amount = EXCLUDED.max_trade_amount,
            max_open_positions = EXCLUDED.max_open_positions,
            target_direction = EXCLUDED.target_direction,
            target_markets_count = EXCLUDED.target_markets_count,
            direction_after_count = EXCLUDED.direction_after_count,
            trade_timeframe = EXCLUDED.trade_timeframe,
            entry_time_before_close = EXCLUDED.entry_time_before_close,
            entry_condition = EXCLUDED.entry_condition,
            entry_price_threshold = EXCLUDED.entry_price_threshold,
            entry_price_max = EXCLUDED.entry_price_max,
            exit_strategy = EXCLUDED.exit_strategy,
            exit_price_threshold = EXCLUDED.exit_price_threshold,
            loss_action = EXCLUDED.loss_action,
            loss_streak_limit = EXCLUDED.loss_streak_limit,
            win_action = EXCLUDED.win_action,
            win_streak_limit = EXCLUDED.win_streak_limit,
            auto_stop_on_low_balance = EXCLUDED.auto_stop_on_low_balance,
            min_balance_threshold = EXCLUDED.min_balance_threshold,
            daily_loss_limit = EXCLUDED.daily_loss_limit,
            allow_retrade = EXCLUDED.allow_retrade,
            updated_at = NOW()
        RETURNING *
        "#,
    )
    .bind(user_id)
    .bind(base_amount)
    .bind(sell_amount)
    .bind(max_trade_amount)
    .bind(max_open_positions)
    .bind(&target_direction)
    .bind(target_markets_count)
    .bind(&direction_after_count)
    .bind(&trade_timeframe)
    .bind(entry_time_before_close)
    .bind(&entry_condition)
    .bind(entry_price_threshold)
    .bind(entry_price_max)
    .bind(&exit_strategy)
    .bind(exit_price_threshold)
    .bind(&loss_action)
    .bind(loss_streak_limit)
    .bind(&win_action)
    .bind(win_streak_limit)
    .bind(auto_stop)
    .bind(min_balance)
    .bind(daily_loss_limit)
    .bind(allow_retrade)
    .bind(current.is_active)
    .fetch_one(pool)
    .await?;

    let cfg = row_to_config(&row);
    cache.insert(user_id.to_string(), cfg.clone());
    Ok(cfg)
}

/// Set is_active flag (start/stop the engine).
pub async fn set_active(
    pool: &PgPool,
    cache: &ConfigCache,
    user_id: &str,
    active: bool,
) -> Result<Option<BotConfig>, sqlx::Error> {
    let row = sqlx::query(
        "UPDATE bot_configs SET is_active = $1, updated_at = NOW() WHERE user_id = $2 RETURNING *",
    )
    .bind(active)
    .bind(user_id)
    .fetch_optional(pool)
    .await?;

    match row {
        Some(row) => {
            let cfg = row_to_config(&row);
            cache.insert(user_id.to_string(), cfg.clone());
            Ok(Some(cfg))
        }
        None => Ok(None),
    }
}

/// Warm the in-memory cache on startup: load all active bot configs from DB.
pub async fn warm_cache(pool: &PgPool, cache: &ConfigCache) {
    match sqlx::query("SELECT * FROM bot_configs")
        .fetch_all(pool)
        .await
    {
        Ok(rows) => {
            for row in &rows {
                let cfg = row_to_config(row);
                cache.insert(cfg.user_id.clone(), cfg);
            }
            tracing::info!("Bot config cache warmed: {} user(s)", cache.len());
        }
        Err(e) => {
            tracing::warn!("Failed to warm bot config cache: {}", e);
        }
    }
}

fn default_config(user_id: &str) -> BotConfig {
    BotConfig {
        user_id: user_id.to_string(),
        base_amount: 10.0,
        sell_amount: None,
        max_trade_amount: None,
        max_open_positions: 1,
        target_direction: None,
        target_markets_count: 1,
        direction_after_count: "any".into(),
        trade_timeframe: "15m".into(),
        entry_time_before_close: 60,
        entry_condition: "lte".into(),
        entry_price_threshold: 0.50,
        entry_price_max: None,
        exit_strategy: "hold".into(),
        exit_price_threshold: None,
        loss_action: "none".into(),
        loss_streak_limit: 0,
        win_action: "none".into(),
        win_streak_limit: 0,
        auto_stop_on_low_balance: true,
        min_balance_threshold: None,
        daily_loss_limit: None,
        allow_retrade: false,
        is_active: false,
        created_at: String::new(),
        updated_at: String::new(),
    }
}

// ─── Config Presets ──────────────────────────────────────────────────────────

pub async fn list_presets(pool: &PgPool, user_id: &str) -> Result<Vec<serde_json::Value>, sqlx::Error> {
    let rows = sqlx::query("SELECT id, name, config_json, created_at, updated_at FROM config_presets WHERE user_id = $1 ORDER BY name")
        .bind(user_id)
        .fetch_all(pool)
        .await?;

    Ok(rows.iter().map(|row| {
        serde_json::json!({
            "id": row.try_get::<String, _>("id").unwrap_or_default(),
            "name": row.try_get::<String, _>("name").unwrap_or_default(),
            "config": row.try_get::<serde_json::Value, _>("config_json").unwrap_or(serde_json::json!({})),
            "created_at": row.try_get::<chrono::DateTime<chrono::Utc>, _>("created_at").map(|dt| dt.to_rfc3339()).unwrap_or_default(),
            "updated_at": row.try_get::<chrono::DateTime<chrono::Utc>, _>("updated_at").map(|dt| dt.to_rfc3339()).unwrap_or_default(),
        })
    }).collect())
}

pub async fn save_preset(pool: &PgPool, user_id: &str, name: &str, config: &BotConfig) -> Result<serde_json::Value, sqlx::Error> {
    let config_json = serde_json::to_value(config).unwrap_or(serde_json::json!({}));
    let row = sqlx::query(
        "INSERT INTO config_presets (id, user_id, name, config_json)
         VALUES (gen_random_uuid()::TEXT, $1, $2, $3)
         ON CONFLICT (user_id, name) DO UPDATE SET config_json = EXCLUDED.config_json, updated_at = NOW()
         RETURNING id, name, config_json, created_at, updated_at"
    )
    .bind(user_id)
    .bind(name)
    .bind(&config_json)
    .fetch_one(pool)
    .await?;

    Ok(serde_json::json!({
        "id": row.try_get::<String, _>("id").unwrap_or_default(),
        "name": row.try_get::<String, _>("name").unwrap_or_default(),
        "config": row.try_get::<serde_json::Value, _>("config_json").unwrap_or(serde_json::json!({})),
        "created_at": row.try_get::<chrono::DateTime<chrono::Utc>, _>("created_at").map(|dt| dt.to_rfc3339()).unwrap_or_default(),
        "updated_at": row.try_get::<chrono::DateTime<chrono::Utc>, _>("updated_at").map(|dt| dt.to_rfc3339()).unwrap_or_default(),
    }))
}

pub async fn delete_preset(pool: &PgPool, preset_id: &str, user_id: &str) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("DELETE FROM config_presets WHERE id = $1 AND user_id = $2")
        .bind(preset_id)
        .bind(user_id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

pub fn row_to_config(row: &sqlx::postgres::PgRow) -> BotConfig {
    BotConfig {
        user_id: row.try_get("user_id").unwrap_or_default(),
        base_amount: row.try_get("base_amount").unwrap_or(10.0),
        sell_amount: row.try_get("sell_amount").ok().flatten(),
        max_trade_amount: row.try_get("max_trade_amount").ok().flatten(),
        max_open_positions: row.try_get("max_open_positions").unwrap_or(1),
        target_direction: row.try_get("target_direction").ok().flatten(),
        target_markets_count: row.try_get("target_markets_count").unwrap_or(1),
        direction_after_count: row.try_get("direction_after_count").unwrap_or_else(|_| "any".into()),
        trade_timeframe: row.try_get("trade_timeframe").unwrap_or_else(|_| "15m".into()),
        entry_time_before_close: row.try_get("entry_time_before_close").unwrap_or(60),
        entry_condition: row.try_get("entry_condition").unwrap_or_else(|_| "lte".into()),
        entry_price_threshold: row.try_get("entry_price_threshold").unwrap_or(0.50),
        entry_price_max: row.try_get("entry_price_max").ok().flatten(),
        exit_strategy: row.try_get("exit_strategy").unwrap_or_else(|_| "hold".into()),
        exit_price_threshold: row.try_get("exit_price_threshold").ok().flatten(),
        loss_action: row.try_get("loss_action").unwrap_or_else(|_| "none".into()),
        loss_streak_limit: row.try_get("loss_streak_limit").unwrap_or(0),
        win_action: row.try_get("win_action").unwrap_or_else(|_| "none".into()),
        win_streak_limit: row.try_get("win_streak_limit").unwrap_or(0),
        auto_stop_on_low_balance: row.try_get("auto_stop_on_low_balance").unwrap_or(true),
        min_balance_threshold: row.try_get("min_balance_threshold").ok().flatten(),
        daily_loss_limit: row.try_get("daily_loss_limit").ok().flatten(),
        allow_retrade: row.try_get("allow_retrade").unwrap_or(false),
        is_active: row.try_get("is_active").unwrap_or(false),
        created_at: row
            .try_get::<chrono::DateTime<chrono::Utc>, _>("created_at")
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_default(),
        updated_at: row
            .try_get::<chrono::DateTime<chrono::Utc>, _>("updated_at")
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_default(),
    }
}
