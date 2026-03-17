use super::types::*;
use reqwest::Client;
use sqlx::Row;
use tracing;

/// Verify a Privy user exists by calling Privy's REST API with Basic auth
pub async fn verify_privy_user(
    http_client: &Client,
    privy_app_id: &str,
    privy_app_secret: &str,
    privy_user_id: &str,
) -> Result<PrivyUserResponse, String> {
    let url = format!("https://auth.privy.io/api/v1/users/{}", privy_user_id);

    let response = http_client
        .get(&url)
        .basic_auth(privy_app_id, Some(privy_app_secret))
        .header("privy-app-id", privy_app_id)
        .send()
        .await
        .map_err(|e| format!("Failed to call Privy API: {}", e))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!(
            "Privy API returned {}: {}",
            status, body
        ));
    }

    response
        .json::<PrivyUserResponse>()
        .await
        .map_err(|e| format!("Failed to parse Privy response: {}", e))
}

/// Create embedded wallet via Privy API
pub async fn create_embedded_wallet(
    http_client: &Client,
    privy_app_id: &str,
    privy_app_secret: &str,
    privy_user_id: &str,
) -> Result<String, String> {
    // First check if user already has a wallet
    let user = verify_privy_user(http_client, privy_app_id, privy_app_secret, privy_user_id).await?;

    // Check linked accounts for existing embedded wallet
    for account in &user.linked_accounts {
        if account.account_type == "wallet" {
            if let Some(ref addr) = account.address {
                tracing::info!("User already has embedded wallet: {}", addr);
                return Ok(addr.clone());
            }
        }
    }

    // Create new embedded wallet
    let url = format!(
        "https://auth.privy.io/api/v1/users/{}/wallets",
        privy_user_id
    );

    let body = serde_json::json!({
        "chain_type": "ethereum",
        "create_additional_wallets": false,
    });

    let response = http_client
        .post(&url)
        .basic_auth(privy_app_id, Some(privy_app_secret))
        .header("privy-app-id", privy_app_id)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("Failed to create embedded wallet: {}", e))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!(
            "Privy wallet creation returned {}: {}",
            status, body
        ));
    }

    let wallet_resp: PrivyCreateWalletResponse = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse wallet response: {}", e))?;

    wallet_resp
        .address
        .ok_or_else(|| "No address in wallet response".to_string())
}

fn row_to_bot_user(row: &sqlx::postgres::PgRow) -> BotUser {
    BotUser {
        id: row.get("id"),
        privy_user_id: row.get("privy_user_id"),
        email: row.get("email"),
        embedded_wallet_address: row.get("embedded_wallet_address"),
        proxy_wallet_address: row.get("proxy_wallet_address"),
        is_active: row.get::<Option<bool>, _>("is_active").unwrap_or(true),
        created_at: row
            .get::<Option<chrono::DateTime<chrono::Utc>>, _>("created_at")
            .map(|t| t.to_rfc3339())
            .unwrap_or_default(),
        updated_at: row
            .get::<Option<chrono::DateTime<chrono::Utc>>, _>("updated_at")
            .map(|t| t.to_rfc3339())
            .unwrap_or_default(),
    }
}

/// Get or create the bot user in the database
pub async fn get_or_create_user(
    db: &sqlx::PgPool,
    privy_user_id: &str,
    email: Option<&str>,
    embedded_wallet_address: Option<&str>,
) -> Result<BotUser, String> {
    // Try to find existing user
    let existing = sqlx::query(
        "SELECT id, privy_user_id, email, embedded_wallet_address, proxy_wallet_address,
                is_active, created_at, updated_at
         FROM bot_users WHERE privy_user_id = $1",
    )
    .bind(privy_user_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("DB query error: {}", e))?;

    if let Some(row) = existing {
        // Update wallet address if provided and different
        if let Some(addr) = embedded_wallet_address {
            let current: Option<String> = row.get("embedded_wallet_address");
            if current.as_deref() != Some(addr) {
                sqlx::query(
                    "UPDATE bot_users SET embedded_wallet_address = $1, updated_at = NOW() WHERE privy_user_id = $2",
                )
                .bind(addr)
                .bind(privy_user_id)
                .execute(db)
                .await
                .map_err(|e| format!("DB update error: {}", e))?;
            }
        }

        let mut user = row_to_bot_user(&row);
        // Override with latest wallet address if provided
        if let Some(addr) = embedded_wallet_address {
            user.embedded_wallet_address = Some(addr.to_string());
        }
        return Ok(user);
    }

    // Create new user
    let row = sqlx::query(
        "INSERT INTO bot_users (privy_user_id, email, embedded_wallet_address)
         VALUES ($1, $2, $3)
         RETURNING id, privy_user_id, email, embedded_wallet_address, proxy_wallet_address,
                   is_active, created_at, updated_at",
    )
    .bind(privy_user_id)
    .bind(email)
    .bind(embedded_wallet_address)
    .fetch_one(db)
    .await
    .map_err(|e| format!("DB insert error: {}", e))?;

    tracing::info!(
        "Created bot user: privy_id={}, email={:?}",
        privy_user_id,
        email
    );

    Ok(row_to_bot_user(&row))
}

/// Update the proxy wallet address for a user
pub async fn set_proxy_wallet(
    db: &sqlx::PgPool,
    privy_user_id: &str,
    proxy_wallet_address: &str,
) -> Result<BotUser, String> {
    let row = sqlx::query(
        "UPDATE bot_users SET proxy_wallet_address = $1, updated_at = NOW()
         WHERE privy_user_id = $2
         RETURNING id, privy_user_id, email, embedded_wallet_address, proxy_wallet_address,
                   is_active, created_at, updated_at",
    )
    .bind(proxy_wallet_address)
    .bind(privy_user_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("DB update error: {}", e))?
    .ok_or_else(|| "User not found".to_string())?;

    Ok(row_to_bot_user(&row))
}

/// Fetch existing user profile from MEVU (including proxy_wallet_address if already deployed).
/// Used when deploy fails with "already deployed" - recover address from MEVU's users table.
pub async fn fetch_proxy_wallet_from_mevu(
    http_client: &Client,
    privy_user_id: &str,
) -> Result<Option<String>, String> {
    let url = std::env::var("MEVU_API_URL").map_err(|_| "MEVU_API_URL not set")?;
    let endpoint = format!(
        "{}/api/users/profiles/{}",
        url.trim_end_matches('/'),
        urlencoding::encode(privy_user_id)
    );

    let response = http_client
        .get(&endpoint)
        .send()
        .await
        .map_err(|e| format!("Failed to call MEVU profiles: {}", e))?;

    if response.status().as_u16() == 404 {
        return Ok(None);
    }

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("MEVU profiles returned {}: {}", status, body));
    }

    #[derive(serde::Deserialize)]
    struct MevuUser {
        #[serde(rename = "proxyWalletAddress")]
        proxy_wallet_address: Option<String>,
    }

    #[derive(serde::Deserialize)]
    struct ProfilesResponse {
        user: MevuUser,
    }

    let resp: ProfilesResponse = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse MEVU profiles response: {}", e))?;

    Ok(resp
        .user
        .proxy_wallet_address
        .filter(|s| !s.is_empty()))
}

/// Deploy proxy wallet via MEVU internal API (Polymarket relayer with rotating keys).
/// Requires MEVU_API_URL. MEVU_INTERNAL_API_KEY is optional (mevu allows private IPs when unset).
pub async fn deploy_proxy_wallet_via_mevu(
    http_client: &Client,
    privy_user_id: &str,
    embedded_wallet_address: &str,
) -> Result<String, String> {
    let url = std::env::var("MEVU_API_URL").map_err(|_| "MEVU_API_URL not set")?;

    let endpoint = url.trim_end_matches('/').to_string() + "/api/internal/deploy-proxy-wallet";

    let body = serde_json::json!({
        "privy_user_id": privy_user_id,
        "embedded_wallet_address": embedded_wallet_address,
    });

    let mut request = http_client
        .post(&endpoint)
        .header("Content-Type", "application/json")
        .json(&body);

    if let Ok(api_key) = std::env::var("MEVU_INTERNAL_API_KEY") {
        if !api_key.is_empty() {
            request = request.header("X-API-Key", api_key);
        }
    }

    let response = request
        .send()
        .await
        .map_err(|e| format!("Failed to call MEVU deploy-proxy-wallet: {}", e))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!(
            "MEVU deploy-proxy-wallet returned {}: {}",
            status, body
        ));
    }

    #[derive(serde::Deserialize)]
    struct DeployResponse {
        proxy_wallet_address: String,
    }

    let resp: DeployResponse = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse MEVU response: {}", e))?;

    Ok(resp.proxy_wallet_address)
}

/// Get the current bot user (there should only be one)
pub async fn get_bot_user(db: &sqlx::PgPool) -> Result<Option<BotUser>, String> {
    let row = sqlx::query(
        "SELECT id, privy_user_id, email, embedded_wallet_address, proxy_wallet_address,
                is_active, created_at, updated_at
         FROM bot_users WHERE is_active = TRUE LIMIT 1",
    )
    .fetch_optional(db)
    .await
    .map_err(|e| format!("DB query error: {}", e))?;

    Ok(row.as_ref().map(row_to_bot_user))
}

/// Get a bot user by their ID
pub async fn get_bot_user_by_id(db: &sqlx::PgPool, user_id: &str) -> Result<Option<BotUser>, String> {
    let row = sqlx::query(
        "SELECT id, privy_user_id, email, embedded_wallet_address, proxy_wallet_address,
                is_active, created_at, updated_at
         FROM bot_users WHERE id = $1",
    )
    .bind(user_id)
    .fetch_optional(db)
    .await
    .map_err(|e| format!("DB query error: {}", e))?;

    Ok(row.as_ref().map(row_to_bot_user))
}
