use serde::{Deserialize, Serialize};

/// A single trade executed by the engine, recorded in DB and kept in memory for the day.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BotTrade {
    pub id: String,
    pub user_id: String,
    pub market_id: String,
    pub market_slug: String,
    pub market_title: String,
    pub timeframe: String,
    /// "up" or "down"
    pub side: String,
    /// USDC amount spent on buy
    pub buy_amount: f64,
    /// Price (0.0–1.0) at time of buy
    pub entry_price: f64,
    /// Price at time of sell/resolution (null if still open)
    pub exit_price: Option<f64>,
    /// USDC received on sell/redeem (null if still open)
    pub payout: Option<f64>,
    /// Profit/loss in USDC (null if still open)
    pub pnl: Option<f64>,
    /// "open" | "won" | "lost" | "sold" | "error"
    pub status: String,
    /// CLOB token ID used for this trade
    pub clob_token_id: String,
    /// Error message if trade failed
    pub error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// Per-user runtime state tracked by the engine (in memory, not persisted across restarts).
#[derive(Debug, Clone)]
pub struct UserEngineState {
    /// Current trade amount (adjusted by streak logic)
    pub current_trade_amount: f64,
    /// Consecutive loss count
    pub loss_streak: i32,
    /// Consecutive win count
    pub win_streak: i32,
    /// How many direction-targeted markets have been traded
    pub direction_markets_traded: i32,
    /// Cached USDC balance (updated after each trade + periodically)
    pub cached_balance: f64,
    /// Market IDs we've already traded today (avoid double-entry)
    pub traded_market_ids: Vec<String>,
    /// Last time we refreshed the balance from MEVU
    pub last_balance_refresh: Option<chrono::DateTime<chrono::Utc>>,
    /// Accumulated realized PnL for today (used for daily loss limit check)
    pub daily_pnl: f64,
}

impl UserEngineState {
    pub fn new(base_amount: f64) -> Self {
        Self {
            current_trade_amount: base_amount,
            loss_streak: 0,
            win_streak: 0,
            direction_markets_traded: 0,
            cached_balance: 0.0,
            traded_market_ids: Vec::new(),
            last_balance_refresh: None,
            daily_pnl: 0.0,
        }
    }

    /// Reset daily counters (called at UTC midnight or on engine start).
    pub fn reset_daily(&mut self, base_amount: f64) {
        self.traded_market_ids.clear();
        self.current_trade_amount = base_amount;
        self.loss_streak = 0;
        self.win_streak = 0;
        self.direction_markets_traded = 0;
        self.daily_pnl = 0.0;
    }
}

/// Result of a buy execution attempt
#[derive(Debug)]
#[allow(dead_code)]
pub enum BuyResult {
    Success {
        /// The trade record to persist
        trade: BotTrade,
    },
    Skipped {
        reason: String,
    },
    Error {
        error: String,
    },
}

/// Result of a sell execution attempt
#[derive(Debug)]
#[allow(dead_code)]
pub enum SellResult {
    Success { payout: f64 },
    Error { error: String },
}

/// Result of a redeem execution attempt
#[derive(Debug)]
#[allow(dead_code)]
pub enum RedeemResult {
    Success { payout: f64 },
    Error { error: String },
}
