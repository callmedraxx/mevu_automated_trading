//! MEVU trade execution: buy, sell, redeem via MEVU API endpoints.
//! These call the MEVU server which handles Privy signing, CLOB order placement,
//! and Polymarket relayer interactions.

use reqwest::Client;
use serde::Deserialize;

fn mevu_base_url() -> Result<String, String> {
    std::env::var("MEVU_API_URL").map_err(|_| "MEVU_API_URL not set".to_string())
}

/// Response from MEVU buy/sell endpoints
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct TradeResponse {
    pub success: Option<bool>,
    pub error: Option<String>,
    pub message: Option<String>,
    /// Order details (varies by endpoint)
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

#[derive(Debug)]
pub struct BuyFill {
    pub fill_price: f64,
    pub fill_amount: f64,
}

/// Execute a BUY order via MEVU POST /api/trading/buy
///
/// MEVU handles: Privy embedded wallet signing → CLOB order (FOK first, then FAK fallback).
/// Returns the fill price and amount on success.
///
/// Note: MEVU's trading API expects the following JSON body:
/// {
///   "privyUserId": "...",
///   "marketInfo": {
///     "marketId": "...",
///     "marketQuestion": "...",
///     "clobTokenId": "...",
///     "outcome": "Yes" | "No" | ...
///   },
///   "orderType": "FOK" | "FAK",
///   "size": "10",   // number of shares
///   "price": "0.5"  // price per share (0-1)
/// }
pub async fn execute_buy(
    http_client: &Client,
    privy_user_id: &str,
    market_id: &str,
    market_question: &str,
    clob_token_id: &str,
    outcome: &str,
    amount_usdc: f64,
    price: f64,
) -> Result<BuyFill, String> {
    let base = mevu_base_url()?;
    let url = format!("{}/api/trading/buy", base.trim_end_matches('/'));

    // Convert our USDC budget into MEVU "size" (shares) = amount / price.
    // Guard against divide-by-zero; MEVU will also validate price range.
    let size_shares = if price > 0.0 { amount_usdc / price } else { 0.0 };

    let market_info = crate::mevu_client::MevuMarketInfo {
        market_id,
        market_question,
        clob_token_id,
        outcome,
    };

    let body = serde_json::json!({
        "privyUserId": privy_user_id,
        "marketInfo": market_info,
        "orderType": "FOK",
        "size": format!("{:.6}", size_shares),
        "price": format!("{:.6}", price),
    });

    tracing::info!(
        "MEVU BUY: user={} market={} token={} amount=${:.2} size={:.4} price={:.4}",
        privy_user_id,
        market_id,
        clob_token_id,
        amount_usdc,
        size_shares,
        price
    );

    let response = http_client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("MEVU buy request failed: {}", e))?;

    let status = response.status();
    let resp_text = response.text().await.unwrap_or_default();

    if !status.is_success() {
        return Err(format!("MEVU buy returned {}: {}", status, resp_text));
    }

    // Parse response — MEVU returns various shapes, we extract what we need
    let parsed: serde_json::Value = serde_json::from_str(&resp_text)
        .map_err(|e| format!("Failed to parse buy response: {} body: {}", e, resp_text))?;

    let success = parsed.get("success").and_then(|v| v.as_bool()).unwrap_or(true);
    if !success {
        let err = parsed.get("error").and_then(|v| v.as_str()).unwrap_or("Unknown error");
        return Err(format!("MEVU buy failed: {}", err));
    }

    // Extract fill info
    let fill_price = parsed
        .get("averagePrice")
        .or_else(|| parsed.get("price"))
        .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .unwrap_or(price);

    let fill_amount = parsed
        .get("filledSize")
        .or_else(|| parsed.get("amount"))
        .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .unwrap_or(amount_usdc);

    tracing::info!(
        "MEVU BUY SUCCESS: user={} fill_price={:.4} fill_amount=${:.2}",
        privy_user_id,
        fill_price,
        fill_amount
    );

    Ok(BuyFill {
        fill_price,
        fill_amount,
    })
}

/// Execute a SELL order via MEVU POST /api/trading/sell
///
/// `size_shares` is the number of shares to sell (from the position's `size` field).
/// Pass the full position size to sell everything, or a fraction for partial sells.
pub async fn execute_sell(
    http_client: &Client,
    privy_user_id: &str,
    market_id: &str,
    market_question: &str,
    clob_token_id: &str,
    outcome: &str,
    size_shares: f64,
    price: f64,
) -> Result<SellFill, String> {
    let base = mevu_base_url()?;
    let url = format!("{}/api/trading/sell", base.trim_end_matches('/'));

    let market_info = crate::mevu_client::MevuMarketInfo {
        market_id,
        market_question,
        clob_token_id,
        outcome,
    };

    let body = serde_json::json!({
        "privyUserId": privy_user_id,
        "marketInfo": market_info,
        "orderType": "FOK",
        "size": format!("{:.6}", size_shares),
        "price": format!("{:.6}", price),
    });

    tracing::info!(
        "MEVU SELL: user={} market={} token={} size={:.6} price={:.4}",
        privy_user_id,
        market_id,
        clob_token_id,
        size_shares,
        price
    );

    let response = http_client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("MEVU sell request failed: {}", e))?;

    let status = response.status();
    let resp_text = response.text().await.unwrap_or_default();

    if !status.is_success() {
        return Err(format!("MEVU sell returned {}: {}", status, resp_text));
    }

    let parsed: serde_json::Value = serde_json::from_str(&resp_text)
        .map_err(|e| format!("Failed to parse sell response: {} body: {}", e, resp_text))?;

    let success = parsed.get("success").and_then(|v| v.as_bool()).unwrap_or(true);
    if !success {
        let err = parsed.get("error").and_then(|v| v.as_str()).unwrap_or("Unknown error");
        return Err(format!("MEVU sell failed: {}", err));
    }

    let fill_price = parsed
        .get("averagePrice")
        .or_else(|| parsed.get("price"))
        .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .unwrap_or(price);

    let payout = parsed
        .get("payout")
        .or_else(|| parsed.get("filledSize"))
        .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .unwrap_or(size_shares * price);

    tracing::info!(
        "MEVU SELL SUCCESS: user={} fill_price={:.4} payout=${:.2}",
        privy_user_id,
        fill_price,
        payout
    );

    Ok(SellFill { fill_price, payout })
}

#[derive(Debug)]
pub struct SellFill {
    pub fill_price: f64,
    pub payout: f64,
}

/// Redeem a resolved position via MEVU POST /api/trading/redeem
///
/// 1) Refresh the MEVU DB by calling `/api/trading/positions` (fetchAndStorePositions)
/// 2) Fetch redeemable positions from MEVU (`/api/trading/redeem/available`) which reads from DB
/// 3) Match by `asset` (clob_token_id) — stable numeric ID, no string format issues
/// 4) Call MEVU `/api/trading/redeem` with privyUserId + conditionId
pub async fn execute_redeem(
    http_client: &Client,
    privy_user_id: &str,
    clob_token_id: &str,
) -> Result<RedeemFill, String> {
    // Trigger a DB refresh first — the redeemable endpoint reads from user_positions table,
    // which only gets updated when /api/trading/positions is called (fetchAndStorePositions).
    // Without this, the redeemable query may return stale data with the old non-redeemable state.
    let _ = crate::mevu_client::fetch_positions(http_client, privy_user_id).await;

    let redeemable = crate::mevu_client::fetch_redeemable_positions(http_client, privy_user_id)
        .await?;

    let maybe_pos = redeemable
        .into_iter()
        .find(|p| p.asset == clob_token_id);

    let position = match maybe_pos {
        Some(p) => p,
        None => {
            return Err(format!(
                "No redeemable position found for asset='{}'",
                clob_token_id
            ));
        }
    };

    let base = mevu_base_url()?;
    let url = format!("{}/api/trading/redeem", base.trim_end_matches('/'));

    let body = serde_json::json!({
        "privyUserId": privy_user_id,
        "conditionId": position.condition_id,
    });

    tracing::info!(
        "MEVU REDEEM: user={} conditionId={} title='{}' outcome='{}'",
        privy_user_id,
        position.condition_id,
        position.title,
        position.outcome
    );

    let response = http_client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("MEVU redeem request failed: {}", e))?;

    let status = response.status();
    let resp_text = response.text().await.unwrap_or_default();

    if !status.is_success() {
        return Err(format!("MEVU redeem returned {}: {}", status, resp_text));
    }

    let parsed: serde_json::Value = serde_json::from_str(&resp_text)
        .map_err(|e| format!("Failed to parse redeem response: {} body: {}", e, resp_text))?;

    let success = parsed.get("success").and_then(|v| v.as_bool()).unwrap_or(true);
    if !success {
        let err = parsed.get("error").and_then(|v| v.as_str()).unwrap_or("Unknown error");
        return Err(format!("MEVU redeem failed: {}", err));
    }

    // Node MEVU returns `redeemedAmount` for successful redemptions.
    let payout = parsed
        .get("redeemedAmount")
        .or_else(|| parsed.get("amount"))
        .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .unwrap_or(0.0);

    tracing::info!(
        "MEVU REDEEM SUCCESS: user={} payout=${:.2} conditionId={}",
        privy_user_id,
        payout,
        position.condition_id
    );

    Ok(RedeemFill { payout })
}

#[derive(Debug)]
pub struct RedeemFill {
    pub payout: f64,
}
