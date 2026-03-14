use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct BotUser {
    pub id: String,
    pub privy_user_id: String,
    pub email: Option<String>,
    pub embedded_wallet_address: Option<String>,
    pub proxy_wallet_address: Option<String>,
    pub is_active: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct RegisterRequest {
    pub privy_user_id: String,
    pub email: Option<String>,
    pub embedded_wallet_address: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SetProxyWalletRequest {
    pub proxy_wallet_address: String,
}

/// Privy API user response (subset of fields we care about)
#[derive(Debug, Deserialize)]
pub struct PrivyUserResponse {
    pub id: String,
    #[serde(default)]
    pub linked_accounts: Vec<PrivyLinkedAccount>,
}

#[derive(Debug, Deserialize)]
pub struct PrivyLinkedAccount {
    #[serde(rename = "type")]
    pub account_type: String,
    pub address: Option<String>,
    #[serde(rename = "verifiedAt")]
    pub verified_at: Option<String>,
}

/// Privy embedded wallet creation response
#[derive(Debug, Deserialize)]
pub struct PrivyCreateWalletResponse {
    pub id: Option<String>,
    pub address: Option<String>,
    pub chain_type: Option<String>,
}
