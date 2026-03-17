use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Full bot configuration for a user. Persisted in DB and cached in memory.
/// The trading engine reads from the in-memory cache on every tick.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BotConfig {
    pub user_id: String,

    // ── Trade Sizing ──────────────────────────────────────────────
    /// USDC amount to spend per buy order (the "base bet")
    pub base_amount: f64,
    /// USDC amount to sell on a winning position (null = sell entire position)
    pub sell_amount: Option<f64>,

    /// Hard cap on any single trade amount (prevents runaway doubling)
    pub max_trade_amount: Option<f64>,
    /// Maximum number of open positions at once (default 1)
    pub max_open_positions: i32,

    // ── Direction Targeting ───────────────────────────────────────
    /// Which outcome to trade: "up", "down", or null (engine picks)
    pub target_direction: Option<String>,
    /// Apply the chosen direction to the next N markets (1–100)
    pub target_markets_count: i32,
    /// What to do after target_markets_count is exhausted: "any" | "stop" | "flip"
    pub direction_after_count: String,

    // ── Timeframe & Timing ─────────────────────────────────────────
    /// Which market timeframe to auto-trade: "5m" | "15m" | "1h" | "4h"
    pub trade_timeframe: String,
    /// Seconds before market close to place the buy order (e.g. 60 = buy 1 min before close).
    /// 0 means "buy as soon as all other conditions are met, regardless of time".
    pub entry_time_before_close: i32,

    // ── Entry Condition ──────────────────────────────────────────
    /// Operator for the entry trigger: "gte" | "lte" | "eq"
    pub entry_condition: String,
    /// Price threshold (0.01–1.0) that the chosen direction's odds must satisfy
    pub entry_price_threshold: f64,
    /// Optional upper bound for entry price. When set, the engine will only trade
    /// if the price is between entry_price_threshold and entry_price_max.
    /// Prevents buying on sudden price spikes past the intended range.
    pub entry_price_max: Option<f64>,

    // ── Exit Strategy ────────────────────────────────────────────
    /// "hold" = let market resolve & claim; "sell_at" = sell when price hits threshold
    pub exit_strategy: String,
    /// Required when exit_strategy = "sell_at" (e.g. 0.99 = sell at 99¢)
    pub exit_price_threshold: Option<f64>,

    // ── Loss-Streak Management ───────────────────────────────────
    /// After a loss: "none" | "double" | "half" the next trade amount
    pub loss_action: String,
    /// Reset to base_amount after this many consecutive losses (0 = never reset)
    pub loss_streak_limit: i32,

    // ── Win-Streak Management ────────────────────────────────────
    /// After a win: "none" | "double" | "half" the next trade amount
    pub win_action: String,
    /// Reset to base_amount after this many consecutive wins (0 = never reset)
    pub win_streak_limit: i32,

    // ── Safety ───────────────────────────────────────────────────
    /// Auto-stop the bot when balance drops below current trade amount
    pub auto_stop_on_low_balance: bool,
    /// Optional hard floor: stop if balance falls below this absolute USDC value
    pub min_balance_threshold: Option<f64>,
    /// Daily loss limit: auto-stop the engine if total realized losses today exceed this amount (USDC)
    pub daily_loss_limit: Option<f64>,

    // ── Re-trade ────────────────────────────────────────────────
    /// Allow the engine to re-enter a market that was already traded successfully.
    /// Default false: once a market has a non-error trade, it won't be traded again.
    pub allow_retrade: bool,

    // ── Engine State ─────────────────────────────────────────────
    /// Whether the trading engine is actively placing trades for this user
    pub is_active: bool,

    pub created_at: String,
    pub updated_at: String,
}

/// Request body for creating or updating a bot config.
/// All fields optional — only provided fields are updated (merge semantics).
#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateBotConfigRequest {
    pub base_amount: Option<f64>,
    pub sell_amount: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_option_option")]
    pub max_trade_amount: Option<Option<f64>>,
    pub max_open_positions: Option<i32>,
    #[serde(default, deserialize_with = "deserialize_option_option")]
    pub target_direction: Option<Option<String>>,
    pub target_markets_count: Option<i32>,
    pub direction_after_count: Option<String>,
    pub trade_timeframe: Option<String>,
    pub entry_time_before_close: Option<i32>,
    pub entry_condition: Option<String>,
    pub entry_price_threshold: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_option_option")]
    pub entry_price_max: Option<Option<f64>>,
    pub exit_strategy: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_option")]
    pub exit_price_threshold: Option<Option<f64>>,
    pub loss_action: Option<String>,
    pub loss_streak_limit: Option<i32>,
    pub win_action: Option<String>,
    pub win_streak_limit: Option<i32>,
    pub auto_stop_on_low_balance: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_option_option")]
    pub min_balance_threshold: Option<Option<f64>>,
    #[serde(default, deserialize_with = "deserialize_option_option")]
    pub daily_loss_limit: Option<Option<f64>>,
    pub allow_retrade: Option<bool>,
}

/// Validation errors returned when starting the bot with invalid config.
#[derive(Debug, Serialize)]
pub struct ValidationErrors {
    pub errors: Vec<String>,
}

/// Validates a BotConfig for completeness before starting the engine.
pub fn validate_config(cfg: &BotConfig) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();

    if cfg.base_amount <= 0.0 {
        errors.push("base_amount must be greater than 0".into());
    }

    if let Some(sell) = cfg.sell_amount {
        if sell <= 0.0 {
            errors.push("sell_amount must be greater than 0 when set".into());
        }
    }

    if let Some(max) = cfg.max_trade_amount {
        if max < cfg.base_amount {
            errors.push("max_trade_amount must be >= base_amount".into());
        }
    }

    if cfg.max_open_positions < 1 || cfg.max_open_positions > 20 {
        errors.push("max_open_positions must be between 1 and 20".into());
    }

    if let Some(sell) = cfg.sell_amount {
        if sell > cfg.base_amount {
            errors.push(format!(
                "sell_amount ({}) should not exceed base_amount ({})",
                sell, cfg.base_amount
            ));
        }
    }

    if let Some(ref dir) = cfg.target_direction {
        if dir != "up" && dir != "down" {
            errors.push("target_direction must be 'up' or 'down'".into());
        }
    }

    if cfg.target_markets_count < 1 || cfg.target_markets_count > 100 {
        errors.push("target_markets_count must be between 1 and 100".into());
    }

    if !["any", "stop", "flip"].contains(&cfg.direction_after_count.as_str()) {
        errors.push("direction_after_count must be 'any', 'stop', or 'flip'".into());
    }

    if !["5m", "15m", "1h", "4h"].contains(&cfg.trade_timeframe.as_str()) {
        errors.push("trade_timeframe must be '5m', '15m', '1h', or '4h'".into());
    }

    if cfg.entry_time_before_close < 0 {
        errors.push("entry_time_before_close must be >= 0".into());
    }

    // Validate entry_time_before_close is reasonable for the chosen timeframe
    let max_seconds = match cfg.trade_timeframe.as_str() {
        "5m" => 300,
        "15m" => 900,
        "1h" => 3600,
        "4h" => 14400,
        _ => 900,
    };
    if cfg.entry_time_before_close > max_seconds {
        errors.push(format!(
            "entry_time_before_close ({}) exceeds the {} timeframe window ({}s)",
            cfg.entry_time_before_close, cfg.trade_timeframe, max_seconds
        ));
    }

    if !["gte", "lte", "eq"].contains(&cfg.entry_condition.as_str()) {
        errors.push("entry_condition must be 'gte', 'lte', or 'eq'".into());
    }

    if cfg.entry_price_threshold < 0.01 || cfg.entry_price_threshold > 1.0 {
        errors.push("entry_price_threshold must be between 0.01 and 1.00".into());
    }

    if let Some(max) = cfg.entry_price_max {
        if max < 0.01 || max > 1.0 {
            errors.push("entry_price_max must be between 0.01 and 1.00".into());
        } else if max < cfg.entry_price_threshold {
            errors.push(format!(
                "entry_price_max ({:.0}¢) must be >= entry_price_threshold ({:.0}¢)",
                max * 100.0, cfg.entry_price_threshold * 100.0
            ));
        }
    }

    if !["hold", "sell_at"].contains(&cfg.exit_strategy.as_str()) {
        errors.push("exit_strategy must be 'hold' or 'sell_at'".into());
    }

    if cfg.exit_strategy == "sell_at" {
        match cfg.exit_price_threshold {
            None => errors.push("exit_price_threshold is required when exit_strategy is 'sell_at'".into()),
            Some(p) if p < 0.01 || p > 1.0 => {
                errors.push("exit_price_threshold must be between 0.01 and 1.00".into());
            }
            _ => {}
        }
    }

    if !["none", "double", "half"].contains(&cfg.loss_action.as_str()) {
        errors.push("loss_action must be 'none', 'double', or 'half'".into());
    }

    if !["none", "double", "half"].contains(&cfg.win_action.as_str()) {
        errors.push("win_action must be 'none', 'double', or 'half'".into());
    }

    if cfg.loss_streak_limit < 0 {
        errors.push("loss_streak_limit must be >= 0".into());
    }

    if cfg.win_streak_limit < 0 {
        errors.push("win_streak_limit must be >= 0".into());
    }

    if let Some(min) = cfg.min_balance_threshold {
        if min < 0.0 {
            errors.push("min_balance_threshold must be >= 0".into());
        }
    }

    if let Some(limit) = cfg.daily_loss_limit {
        if limit <= 0.0 {
            errors.push("daily_loss_limit must be greater than 0".into());
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Custom deserializer for Option<Option<T>>:
/// - field absent  → outer None (don't touch this field)
/// - field: null   → Some(None) (explicitly set to null)
/// - field: value  → Some(Some(value))
fn deserialize_option_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    let opt: Option<T> = Option::deserialize(deserializer)?;
    Ok(Some(opt))
}
