//! Gamma API types for Polymarket crypto markets.
//! Based on https://github.com/callmedraxx/mevu

#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Gamma API pagination response for /events/pagination
#[derive(Debug, Deserialize)]
pub struct GammaPaginationResponse {
    pub data: Vec<GammaEvent>,
    #[serde(rename = "next_cursor")]
    pub next_cursor: Option<String>,
    pub count: Option<u64>,
}

/// Gamma API event (crypto market event)
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GammaEvent {
    pub id: String,
    pub ticker: Option<String>,
    pub slug: String,
    pub title: String,
    pub description: Option<String>,
    pub resolution_source: Option<String>,
    pub start_date: Option<String>,
    pub creation_date: Option<String>,
    pub end_date: Option<String>,
    pub image: Option<String>,
    pub icon: Option<String>,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    pub archived: Option<bool>,
    #[serde(rename = "new")]
    pub is_new: Option<bool>,
    pub featured: Option<bool>,
    pub restricted: Option<bool>,
    pub liquidity: Option<f64>,
    pub volume: Option<f64>,
    pub open_interest: Option<f64>,
    pub competitive: Option<f64>,
    pub enable_order_book: Option<bool>,
    pub liquidity_clob: Option<f64>,
    pub neg_risk: Option<bool>,
    pub comment_count: Option<u32>,
    pub cyom: Option<bool>,
    pub show_all_outcomes: Option<bool>,
    pub show_market_images: Option<bool>,
    pub automatically_active: Option<bool>,
    pub neg_risk_augmented: Option<bool>,
    pub pending_deployment: Option<bool>,
    pub deploying: Option<bool>,
    pub start_time: Option<String>,
    pub series_slug: Option<String>,
    pub markets: Option<Vec<GammaMarket>>,
    pub series: Option<serde_json::Value>,
    pub tags: Option<Vec<GammaTag>>,
}

/// Gamma API market (sub-market within an event)
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GammaMarket {
    pub id: String,
    pub question: Option<String>,
    pub condition_id: Option<String>,
    pub slug: Option<String>,
    /// JSON string or array - outcomes like ["Yes", "No"] or ["Up", "Down"]
    pub outcomes: Option<serde_json::Value>,
    /// JSON string or array - outcome prices
    pub outcome_prices: Option<serde_json::Value>,
    /// JSON string or array - CLOB token IDs for orderbook
    pub clob_token_ids: Option<serde_json::Value>,
}

/// Gamma API tag (for timeframe/asset categorization)
#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct GammaTag {
    pub id: Option<String>,
    pub label: Option<String>,
    pub slug: String,
}

/// Market direction (up or down prediction)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Up,
    Down,
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Direction::Up => write!(f, "up"),
            Direction::Down => write!(f, "down"),
        }
    }
}

/// Supported timeframes for crypto markets
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, ToSchema)]
pub enum Timeframe {
    #[serde(rename = "5m")]
    FiveMin,
    #[serde(rename = "15m")]
    FifteenMin,
    #[serde(rename = "1h")]
    OneHour,
    #[serde(rename = "4h")]
    FourHour,
    #[serde(rename = "weekly")]
    Weekly,
}

impl Timeframe {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "5m" | "5min" => Some(Self::FiveMin),
            "15m" | "15min" => Some(Self::FifteenMin),
            "1h" | "1hour" => Some(Self::OneHour),
            "4h" | "4hour" => Some(Self::FourHour),
            "weekly" => Some(Self::Weekly),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FiveMin => "5m",
            Self::FifteenMin => "15m",
            Self::OneHour => "1h",
            Self::FourHour => "4h",
            Self::Weekly => "weekly",
        }
    }
}

impl std::fmt::Display for Timeframe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Transformed crypto market for API responses
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CryptoMarket {
    pub id: String,
    pub slug: String,
    pub title: String,
    pub timeframe: Option<Timeframe>,
    pub asset: Option<String>,
    pub direction: Option<Direction>,
    pub up_price: Option<f64>,
    pub down_price: Option<f64>,
    pub volume: Option<f64>,
    pub liquidity: Option<f64>,
    pub series_slug: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub start_time: Option<String>,
    pub up_clob_token_id: Option<String>,
    pub down_clob_token_id: Option<String>,
}

/// Query params for filtering markets
#[derive(Debug, Deserialize, ToSchema)]
pub struct MarketFilter {
    pub direction: Option<String>,
    pub timeframe: Option<String>,
    pub asset: Option<String>,
}

// ── CLOB WebSocket types ──

/// CLOB WebSocket price_change event
#[derive(Debug, Deserialize)]
pub struct ClobPriceChangeEvent {
    pub event_type: String,
    pub market: Option<String>,
    pub price_changes: Option<Vec<ClobPriceChange>>,
    pub timestamp: Option<String>,
}

/// Individual price change within a price_change event
#[derive(Debug, Deserialize)]
pub struct ClobPriceChange {
    pub asset_id: Option<String>,
    pub best_ask: Option<String>,
    pub best_bid: Option<String>,
    pub price: Option<String>,
    pub side: Option<String>,
    pub size: Option<String>,
}

/// Live price snapshot stored in memory for each token
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct LivePrice {
    pub clob_token_id: String,
    pub slug: String,
    pub outcome: String,       // "up" or "down"
    pub best_bid: f64,
    pub best_ask: f64,
    pub price_cents: u32,      // best_bid * 100
    pub updated_at_ms: u64,
}

/// API response for live prices of a market
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MarketLivePrices {
    pub slug: String,
    pub up_price: Option<LivePrice>,
    pub down_price: Option<LivePrice>,
}
