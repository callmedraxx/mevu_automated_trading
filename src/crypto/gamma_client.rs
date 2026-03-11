use crate::crypto::types::GammaEvent;

const GAMMA_BASE: &str = "https://gamma-api.polymarket.com";
const CRYPTO_TAG_ID: u32 = 21;
const PAGE_LIMIT: u32 = 100;

pub async fn fetch_all_crypto_events(
    client: &reqwest::Client,
) -> Result<Vec<GammaEvent>, Box<dyn std::error::Error + Send + Sync>> {
    let mut all = Vec::new();
    let mut offset = 0;

    loop {
        let url = format!(
            "{}/events/pagination?tag_id={}&active=true&closed=false&limit={}&offset={}&order=volume24hr&ascending=false",
            GAMMA_BASE, CRYPTO_TAG_ID, PAGE_LIMIT, offset
        );
        let resp: serde_json::Value = client.get(&url).send().await?.json().await?;

        let events: Vec<GammaEvent> = serde_json::from_value(
            resp.get("data")
                .cloned()
                .unwrap_or(serde_json::Value::Array(vec![])),
        )?;

        if events.is_empty() {
            break;
        }

        let len = events.len();
        all.extend(events);
        offset += PAGE_LIMIT;

        if len < PAGE_LIMIT as usize {
            break;
        }
    }

    Ok(all)
}
