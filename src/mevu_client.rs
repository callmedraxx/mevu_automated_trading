//! MEVU API client - fetches balance, positions, trades, PnL from MEVU

use reqwest::Client;
use serde::{Deserialize, Serialize};

fn mevu_base_url() -> Result<String, String> {
    std::env::var("MEVU_API_URL").map_err(|_| "MEVU_API_URL not set".to_string())
}

/// Fetch balance from MEVU GET /api/balances/:privyUserId
pub async fn fetch_balance(
    http_client: &Client,
    privy_user_id: &str,
) -> Result<BalanceResponse, String> {
    let base = mevu_base_url()?;
    let url = format!(
        "{}/api/balances/{}",
        base.trim_end_matches('/'),
        urlencoding::encode(privy_user_id)
    );

    let response = http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("MEVU balance fetch failed: {}", e))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("MEVU balance returned {}: {}", status, body));
    }

    response
        .json()
        .await
        .map_err(|e| format!("Failed to parse MEVU balance: {}", e))
}

/// Shape of the market info object expected by MEVU's /api/trading/buy and /api/trading/sell
#[derive(Debug, Serialize)]
pub struct MevuMarketInfo<'a> {
    #[serde(rename = "marketId")]
    pub market_id: &'a str,
    #[serde(rename = "marketQuestion")]
    pub market_question: &'a str,
    #[serde(rename = "clobTokenId")]
    pub clob_token_id: &'a str,
    pub outcome: &'a str,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BalanceResponse {
    pub success: bool,
    pub balance: Option<String>,
    #[serde(rename = "humanBalance")]
    pub human_balance: Option<String>,
}

/// Fetch positions from MEVU GET /api/trading/positions?privyUserId=...
pub async fn fetch_positions(
    http_client: &Client,
    privy_user_id: &str,
) -> Result<Vec<Position>, String> {
    let base = mevu_base_url()?;
    let url = format!(
        "{}/api/trading/positions?privyUserId={}",
        base.trim_end_matches('/'),
        urlencoding::encode(privy_user_id)
    );

    let response = http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("MEVU positions fetch failed: {}", e))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("MEVU positions returned {}: {}", status, body));
    }

    #[derive(Deserialize)]
    struct Wrapper {
        positions: Option<Vec<Position>>,
    }

    let wrapper: Wrapper = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse MEVU positions: {}", e))?;

    Ok(wrapper.positions.unwrap_or_default())
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Position {
    pub id: Option<String>,
    pub title: Option<String>,
    pub outcome: Option<String>,
    pub asset: Option<String>,
    #[serde(rename = "conditionId")]
    pub condition_id: Option<String>,
    pub size: Option<String>,
    #[serde(rename = "currentValue")]
    pub current_value: Option<String>,
    #[serde(rename = "cashPnl")]
    pub cash_pnl: Option<String>,
    #[serde(rename = "percentPnl")]
    pub percent_pnl: Option<String>,
    pub redeemable: Option<bool>,
}

/// Redeemable position returned by MEVU GET /api/trading/redeem/available
#[derive(Debug, Deserialize, Clone)]
pub struct RedeemablePosition {
    pub asset: String,
    #[serde(rename = "conditionId")]
    pub condition_id: String,
    pub size: String,
    #[serde(rename = "currentValue")]
    pub current_value: String,
    pub title: String,
    pub outcome: String,
    #[serde(rename = "eventId")]
    pub event_id: Option<String>,
}

/// Fetch redeemable positions from MEVU GET /api/trading/redeem/available?privyUserId=...
pub async fn fetch_redeemable_positions(
    http_client: &Client,
    privy_user_id: &str,
) -> Result<Vec<RedeemablePosition>, String> {
    let base = mevu_base_url()?;
    let url = format!(
        "{}/api/trading/redeem/available?privyUserId={}",
        base.trim_end_matches('/'),
        urlencoding::encode(privy_user_id)
    );

    let response = http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("MEVU redeemable positions fetch failed: {}", e))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!(
            "MEVU redeemable positions returned {}: {}",
            status, body
        ));
    }

    #[derive(Deserialize)]
    struct Wrapper {
        positions: Option<Vec<RedeemablePosition>>,
    }

    let wrapper: Wrapper = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse MEVU redeemable positions: {}", e))?;

    Ok(wrapper.positions.unwrap_or_default())
}

/// Fetch PnL snapshot from MEVU GET /api/positions/:privyUserId/pnl/current
pub async fn fetch_pnl_current(
    http_client: &Client,
    privy_user_id: &str,
) -> Result<PnlSnapshot, String> {
    let base = mevu_base_url()?;
    let url = format!(
        "{}/api/positions/{}/pnl/current",
        base.trim_end_matches('/'),
        urlencoding::encode(privy_user_id)
    );

    let response = http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("MEVU PnL fetch failed: {}", e))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("MEVU PnL returned {}: {}", status, body));
    }

    response
        .json()
        .await
        .map_err(|e| format!("Failed to parse MEVU PnL: {}", e))
}

#[derive(Debug, Deserialize)]
pub struct PnlSnapshot {
    #[serde(rename = "totalPnl")]
    pub total_pnl: Option<f64>,
    #[serde(rename = "realizedPnl")]
    pub realized_pnl: Option<f64>,
    #[serde(rename = "unrealizedPnl")]
    pub unrealized_pnl: Option<f64>,
    #[serde(rename = "portfolioValue")]
    pub portfolio_value: Option<f64>,
    #[serde(rename = "usdcBalance")]
    pub usdc_balance: Option<f64>,
}

/// Compute winrate from positions (Rust-side logic).
/// Wins = redeemable positions. Losses = positions with negative cashPnl that are resolved (not redeemable).
pub fn compute_winrate_from_positions(positions: &[Position]) -> f64 {
    let mut wins = 0u32;
    let mut losses = 0u32;

    for p in positions {
        if p.redeemable == Some(true) {
            wins += 1;
        } else if let Some(ref cash_pnl) = p.cash_pnl {
            if let Ok(val) = cash_pnl.parse::<f64>() {
                if val < -0.001 {
                    losses += 1;
                }
            }
        }
    }

    let total = wins + losses;
    if total == 0 {
        return 0.0;
    }
    (wins as f64 / total as f64) * 100.0
}

/// Helper outcome mapper for crypto up/down markets.
/// MEVU's trading API expects a human-readable outcome string; for our
/// crypto up/down markets we map:
/// - "up"   → "Yes"
/// - "down" → "No"
/// - any other fallback → "Yes"
pub fn outcome_from_side(side: &str) -> &'static str {
    match side {
        "up" => "Yes",
        "down" => "No",
        _ => "Yes",
    }
}
