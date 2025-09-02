SET work_mem = '256MB';
SET maintenance_work_mem = '1GB';
SET shared_buffers = '512MB';
SET autocommit = off;

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    telegram_id BIGINT UNIQUE NOT NULL,
    username VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    is_admin BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    session_token VARCHAR(255) UNIQUE NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS whitelist (
    telegram_id BIGINT PRIMARY KEY,
    added_by UUID REFERENCES users(id) ON DELETE SET NULL,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS groups (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) UNIQUE NOT NULL,
    created_by UUID REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS wallets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    address VARCHAR(44) UNIQUE NOT NULL,
    name VARCHAR(255),
    group_id UUID REFERENCES groups(id) ON DELETE SET NULL,
    added_by UUID REFERENCES users(id) ON DELETE SET NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    mint VARCHAR(44) UNIQUE NOT NULL,
    symbol VARCHAR(50),
    name VARCHAR(255),
    decimals INTEGER,
    deployment_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_id UUID REFERENCES wallets(id) ON DELETE CASCADE, 
    signature VARCHAR(88) UNIQUE NOT NULL,
    block_time TIMESTAMP NOT NULL,
    transaction_type VARCHAR(20) NOT NULL,
    sol_spent NUMERIC DEFAULT 0 NOT NULL,
    sol_received NUMERIC DEFAULT 0 NOT NULL,
    usd_spent NUMERIC DEFAULT 0 NOT NULL,
    usd_received NUMERIC DEFAULT 0 NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS token_operations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id UUID REFERENCES transactions(id) ON DELETE CASCADE, 
    token_id UUID REFERENCES tokens(id) ON DELETE CASCADE,
    amount NUMERIC NOT NULL,
    operation_type VARCHAR(20) NOT NULL,
    token_price_usd NUMERIC,
    sol_price_usd NUMERIC,
    sol_amount NUMERIC,
    usd_value NUMERIC,
    market_cap NUMERIC,
    deployment_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS wallet_stats (
    wallet_id UUID PRIMARY KEY REFERENCES wallets(id) ON DELETE CASCADE, 
    total_spent_sol NUMERIC DEFAULT 0,
    total_received_sol NUMERIC DEFAULT 0,
    total_buy_transactions INTEGER DEFAULT 0,
    total_sell_transactions INTEGER DEFAULT 0,
    unique_tokens_bought INTEGER DEFAULT 0,
    unique_tokens_sold INTEGER DEFAULT 0,
    last_transaction_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS monitoring_stats (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    processed_signatures INTEGER,
    total_wallets_monitored INTEGER,
    last_scan_duration INTEGER,
    errors_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_wallets_address ON wallets(address);
CREATE INDEX IF NOT EXISTS idx_wallets_group_id ON wallets(group_id);
CREATE INDEX IF NOT EXISTS idx_wallets_added_by ON wallets(added_by);
CREATE INDEX IF NOT EXISTS idx_wallets_is_active ON wallets(is_active);
CREATE INDEX IF NOT EXISTS idx_transactions_wallet_id ON transactions(wallet_id);
CREATE INDEX IF NOT EXISTS idx_transactions_block_time ON transactions(block_time);
CREATE INDEX IF NOT EXISTS idx_transactions_signature ON transactions(signature);
CREATE INDEX IF NOT EXISTS idx_transactions_type_time ON transactions(transaction_type, block_time);
CREATE INDEX IF NOT EXISTS idx_token_operations_transaction_id ON token_operations(transaction_id);
CREATE INDEX IF NOT EXISTS idx_token_operations_token_id ON token_operations(token_id);
CREATE INDEX IF NOT EXISTS idx_token_operations_type ON token_operations(operation_type);
CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(session_token);
CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);
CREATE INDEX IF NOT EXISTS idx_whitelist_telegram_id ON whitelist(telegram_id);
CREATE INDEX IF NOT EXISTS idx_groups_name ON groups(name);
CREATE INDEX IF NOT EXISTS idx_tokens_mint ON tokens(mint);
CREATE INDEX IF NOT EXISTS idx_wallet_stats_wallet_id ON wallet_stats(wallet_id);

CREATE INDEX IF NOT EXISTS idx_transactions_wallet_time ON transactions(wallet_id, block_time DESC);
CREATE INDEX IF NOT EXISTS idx_token_ops_tx_token ON token_operations(transaction_id, token_id);
CREATE INDEX IF NOT EXISTS idx_wallets_group_active ON wallets(group_id, is_active) WHERE is_active = true;

INSERT INTO users (telegram_id, username, first_name, is_admin, is_active)
VALUES (789676557, 'admin', 'Admin', true, true)
ON CONFLICT (telegram_id) DO UPDATE SET
    is_admin = true,
    is_active = true;

INSERT INTO whitelist (telegram_id, notes)
VALUES (789676557, 'Initial admin user')
ON CONFLICT (telegram_id) DO NOTHING;