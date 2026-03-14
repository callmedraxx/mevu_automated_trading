CREATE TABLE IF NOT EXISTS bot_users (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
    privy_user_id TEXT UNIQUE NOT NULL,
    email TEXT,
    embedded_wallet_address TEXT,
    proxy_wallet_address TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bot_users_privy_user_id ON bot_users(privy_user_id);
CREATE INDEX IF NOT EXISTS idx_bot_users_embedded_wallet ON bot_users(embedded_wallet_address);
