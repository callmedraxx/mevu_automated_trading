//! Gamma API types for Polymarket crypto markets.
//! Based on https://github.com/callmedraxx/mevu

#![allow(dead_code)]

use serde::Deserialize;

/// Gamma API pagination response for /events/pagination
#[derive(Debug, Deserialize)]
pub struct GammaPaginationResponse {
    pub data: Vec<GammaEvent>,
    #[serde(rename = "next_cursor")]
    pub next_cursor: Option<String>,
    pub count: Option<u64>,
}

/// Gamma API event (crypto market event)
#[derive(Debug, Deserialize)]
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
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GammaMarket {
    pub id: String,
    pub question: Option<String>,
    pub condition_id: Option<String>,
    pub slug: Option<String>,
    /// JSON string or array - outcomes like ["Yes", "No"]
    pub outcomes: Option<serde_json::Value>,
    /// JSON string or array - outcome prices
    pub outcome_prices: Option<serde_json::Value>,
    /// JSON string or array - CLOB token IDs for orderbook
    pub clob_token_ids: Option<serde_json::Value>,
}

/// Gamma API tag (for timeframe/asset categorization)
#[derive(Debug, Deserialize, Clone)]
pub struct GammaTag {
    pub id: Option<String>,
    pub label: Option<String>,
    pub slug: String,
}

/// Transformed crypto market (internal representation for cache/API)
#[derive(Debug, Clone)]
pub struct CryptoMarket {
    pub id: String,
    pub slug: String,
    pub title: String,
    pub timeframe: Option<String>,
    pub asset: Option<String>,
}
