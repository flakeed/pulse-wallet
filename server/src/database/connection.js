const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

class Database {
    constructor() {
        this.pool = new Pool({
            connectionString: process.env.DATABASE_URL,
        });

        this.priceService = null;

        this.pool.on('error', (err) => {
            console.error('❌ Unexpected error on idle PostgreSQL client', err);
        });

        this.initDatabase();
    }

    async initDatabase() {
        try {
            const client = await this.pool.connect();
            console.log('✅ Connected to PostgreSQL database');
            client.release();
            await this.createSchema();
        } catch (error) {
            console.error('❌ Database connection error:', error.message);
            throw error;
        }
    }

    async createSchema() {
        try {
            const schemaPath = path.join(__dirname, 'schema.sql');
            const schema = fs.readFileSync(schemaPath, 'utf8');
            const statements = schema.split(';').map(stmt => stmt.trim()).filter(stmt => stmt.length > 0);
            const client = await this.pool.connect();
            try {
                for (const statement of statements) {
                    try {
                        await client.query(statement);
                    } catch (err) {
                    }
                }
                await client.query(`
                    ALTER TABLE token_operations
                    ADD COLUMN IF NOT EXISTS token_price_usd NUMERIC,
                    ADD COLUMN IF NOT EXISTS sol_price_usd NUMERIC,
                    ADD COLUMN IF NOT EXISTS sol_amount NUMERIC,
                    ADD COLUMN IF NOT EXISTS usd_value NUMERIC,
                    ADD COLUMN IF NOT EXISTS market_cap NUMERIC,
                    ADD COLUMN IF NOT EXISTS deployment_time TIMESTAMP;
                `);
                console.log('✅ Database schema initialized');
            } finally {
                client.release();
            }
        } catch (error) {
            console.error('❌ Error creating schema:', error.message);
            throw error;
        }
    }

    async addGroup(name, createdBy = null) {
        const query = `
            INSERT INTO groups (name, created_by)
            VALUES ($1, $2)
            RETURNING id, name, created_by, created_at
        `;
        try {
            const result = await this.pool.query(query, [name, createdBy]);
            return result.rows[0];
        } catch (error) {
            if (error.code === '23505') {
                throw new Error('Group name already exists');
            }
            throw error;
        }
    }

    async getGroups() {
        const query = `
            SELECT g.id, g.name, COUNT(w.id) as wallet_count, g.created_by, g.created_at,
                   u.username as created_by_username, u.first_name as created_by_name
            FROM groups g
            LEFT JOIN wallets w ON g.id = w.group_id AND w.is_active = true
            LEFT JOIN users u ON g.created_by = u.id
            GROUP BY g.id, g.name, g.created_by, g.created_at, u.username, u.first_name
            ORDER BY g.created_at
        `;
        const result = await this.pool.query(query);
        return result.rows;
    }

    async addWalletsBatchOptimized(wallets) {
        if (!wallets || wallets.length === 0) {
            throw new Error('Wallets array is required');
        }
    
        const maxBatchSize = 1000;
        if (wallets.length > maxBatchSize) {
            throw new Error(`Batch size too large. Maximum ${maxBatchSize} wallets per batch.`);
        }
    
        const startTime = Date.now();
        console.log(`[${new Date().toISOString()}] 🚀 Starting global batch insert: ${wallets.length} wallets`);
    
        try {
            const client = await this.pool.connect();
            
            try {
                await client.query('BEGIN');
    
                const values = [];
                const placeholders = [];
                
                wallets.forEach((wallet, index) => {
                    const offset = index * 4;
                    placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}::uuid, $${offset + 4}::uuid)`);
                    values.push(
                        wallet.address,
                        wallet.name || null,
                        wallet.groupId || null,
                        wallet.addedBy || null
                    );
                });
    
                const insertQuery = `
                    INSERT INTO wallets (address, name, group_id, added_by)
                    VALUES ${placeholders.join(', ')}
                    ON CONFLICT (address) DO NOTHING
                    RETURNING id, address, name, group_id, added_by, created_at
                `;
    
                const insertResult = await client.query(insertQuery, values);
                await client.query('COMMIT');

                const insertTime = Date.now() - startTime;
                console.log(`[${new Date().toISOString()}] ✅ Global batch insert completed in ${insertTime}ms: ${insertResult.rows.length} wallets`);
                
                return insertResult.rows;
    
            } catch (error) {
                await client.query('ROLLBACK');
                throw error;
            } finally {
                client.release();
            }
    
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Global batch insert failed:`, error.message);
            throw new Error(`Global batch insert failed: ${error.message}`);
        }
    }

    async removeAllWallets(groupId = null) {
        const startTime = Date.now();
        console.log(`[${new Date().toISOString()}] 🗑️ Starting complete wallet removal${groupId ? ` for group ${groupId}` : ' (ALL GROUPS)'}`);
        
        try {
            const client = await this.pool.connect();
            
            try {
                await client.query('BEGIN');
                
                let walletQuery = `
                    SELECT w.id, w.address, w.name, w.group_id, g.name as group_name,
                           COUNT(DISTINCT t.id) as transaction_count
                    FROM wallets w
                    LEFT JOIN groups g ON w.group_id = g.id
                    LEFT JOIN transactions t ON w.id = t.wallet_id
                    WHERE w.is_active = true
                `;
                const params = [];
                
                if (groupId) {
                    walletQuery += ` AND w.group_id = $1`;
                    params.push(groupId);
                }
                
                walletQuery += ` GROUP BY w.id, w.address, w.name, w.group_id, g.name ORDER BY w.created_at`;
                
                const walletsToDelete = await client.query(walletQuery, params);
                const wallets = walletsToDelete.rows;
                
                if (wallets.length === 0) {
                    await client.query('COMMIT');
                    console.log(`[${new Date().toISOString()}] ℹ️ No wallets found to delete${groupId ? ` in group ${groupId}` : ''}`);
                    return { deletedCount: 0, message: 'No wallets found to delete' };
                }
                
                const walletIds = wallets.map(w => w.id);
                console.log(`[${new Date().toISOString()}] 📊 Found ${wallets.length} wallets to delete with ${wallets.reduce((sum, w) => sum + parseInt(w.transaction_count), 0)} total transactions`);
                
                const tokenOpsQuery = `
                    DELETE FROM token_operations 
                    WHERE transaction_id IN (
                        SELECT id FROM transactions WHERE wallet_id = ANY($1)
                    )
                `;
                const tokenOpsResult = await client.query(tokenOpsQuery, [walletIds]);
                console.log(`[${new Date().toISOString()}] ✅ Deleted ${tokenOpsResult.rowCount} token operations`);
                
                const transactionsQuery = `DELETE FROM transactions WHERE wallet_id = ANY($1)`;
                const transactionsResult = await client.query(transactionsQuery, [walletIds]);
                console.log(`[${new Date().toISOString()}] ✅ Deleted ${transactionsResult.rowCount} transactions`);
                
                const statsQuery = `DELETE FROM wallet_stats WHERE wallet_id = ANY($1)`;
                const statsResult = await client.query(statsQuery, [walletIds]);
                console.log(`[${new Date().toISOString()}] ✅ Deleted ${statsResult.rowCount} wallet stats`);
                
                let deleteWalletsQuery = `DELETE FROM wallets WHERE id = ANY($1)`;
                const deleteWalletsResult = await client.query(deleteWalletsQuery, [walletIds]);
                console.log(`[${new Date().toISOString()}] ✅ Deleted ${deleteWalletsResult.rowCount} wallets`);
                
                await client.query('COMMIT');
                
                const duration = Date.now() - startTime;
                const deletionReport = {
                    wallets: deleteWalletsResult.rowCount,
                    transactions: transactionsResult.rowCount,
                    tokenOperations: tokenOpsResult.rowCount,
                    walletStats: statsResult.rowCount,
                    duration: `${duration}ms`,
                    groupId: groupId,
                    deletedWalletAddresses: wallets.map(w => w.address)
                };
                
                console.log(`[${new Date().toISOString()}] 🎉 Complete wallet removal finished in ${duration}ms:`, deletionReport);
                
                return { 
                    deletedCount: deleteWalletsResult.rowCount,
                    message: `Successfully removed ${deleteWalletsResult.rowCount} wallets and all associated data`,
                    details: deletionReport
                };
                
            } catch (error) {
                await client.query('ROLLBACK');
                console.error(`[${new Date().toISOString()}] ❌ Transaction rollback due to error:`, error);
                throw error;
            } finally {
                client.release();
            }
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error in complete wallet removal:`, error);
            throw new Error(`Failed to remove wallets: ${error.message}`);
        }
    }

    async getActiveWallets(groupId = null) {
        let query = `
            SELECT w.*, g.name as group_name, u.username as added_by_username
            FROM wallets w
            LEFT JOIN groups g ON w.group_id = g.id
            LEFT JOIN users u ON w.added_by = u.id
            WHERE w.is_active = TRUE
        `;
        const params = [];
        
        if (groupId) {
            query += ` AND w.group_id = $1`;
            params.push(groupId);
        }
        
        query += ` ORDER BY w.created_at DESC`;
        
        const result = await this.pool.query(query, params);
        console.log(`[${new Date().toISOString()}] 📊 Found ${result.rows.length} active wallets globally${groupId ? ` for group ${groupId}` : ''}`);
        
        return result.rows;
    }

    async getWalletCount(groupId = null) {
        try {
            const startTime = Date.now();
    
            let query;
            let params = [];
    
            if (groupId) {
                query = `
                    SELECT 
                        COUNT(*) as total_wallets,
                        $1::uuid as group_id,
                        g.name as group_name,
                        COUNT(*) as wallet_count
                    FROM wallets w
                    LEFT JOIN groups g ON w.group_id = g.id
                    WHERE w.is_active = true AND w.group_id = $1::uuid
                    GROUP BY g.name
                `;
                params = [groupId];
            } else {
                query = `
                    WITH group_counts AS (
                        SELECT 
                            w.group_id,
                            COALESCE(g.name, 'No Group') as group_name,
                            COUNT(*) as wallet_count
                        FROM wallets w
                        LEFT JOIN groups g ON w.group_id = g.id
                        WHERE w.is_active = true
                        GROUP BY w.group_id, g.name
                    ),
                    total_count AS (
                        SELECT SUM(wallet_count) as total_wallets
                        FROM group_counts
                    )
                    SELECT 
                        tc.total_wallets,
                        gc.group_id,
                        gc.group_name,
                        gc.wallet_count
                    FROM total_count tc
                    CROSS JOIN group_counts gc
                    UNION ALL
                    SELECT 
                        tc.total_wallets,
                        NULL as group_id,
                        'TOTAL' as group_name,
                        tc.total_wallets as wallet_count
                    FROM total_count tc
                    ORDER BY group_id NULLS LAST
                `;
            }
    
            const result = await this.pool.query(query, params);
            const duration = Date.now() - startTime;
    
            if (groupId) {
                const groupData = result.rows[0];
                const totalWallets = groupData ? parseInt(groupData.total_wallets || 0) : 0;
                
                return {
                    totalWallets: totalWallets,
                    selectedGroup: groupData ? {
                        groupId: groupData.group_id,
                        walletCount: parseInt(groupData.wallet_count || 0),
                        groupName: groupData.group_name
                    } : null,
                    groups: []
                };
            } else {
                const totalRow = result.rows.find(row => row.group_name === 'TOTAL');
                const totalWallets = totalRow ? parseInt(totalRow.total_wallets || 0) : 0;
                
                const groups = result.rows
                    .filter(row => row.group_name !== 'TOTAL')
                    .map(row => ({
                        groupId: row.group_id,
                        groupName: row.group_name,
                        walletCount: parseInt(row.wallet_count || 0)
                    }));
    
                return {
                    totalWallets: totalWallets,
                    selectedGroup: null,
                    groups: groups
                };
            }
    
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error in wallet count:`, error);
            throw new Error(`Failed to get wallet count: ${error.message}`);
        }
    }

    async getRecentTransactionsOptimized(hours = 24, limit = 400, transactionType = null, groupId = null) {
        try {
            console.log(`[${new Date().toISOString()}] 🚀 Transactions fetch: ${hours}h, limit ${limit}${groupId ? `, group ${groupId}` : ''}`);
            const startTime = Date.now();

            let typeFilter = '';
            let queryParams = [limit];
            let paramIndex = 2;
            
            if (transactionType) {
                typeFilter = `AND t.transaction_type = ${paramIndex++}`;
                queryParams.push(transactionType);
            }
            if (groupId) {
                typeFilter += ` AND w.group_id = ${paramIndex++}`;
                queryParams.push(groupId);
            }

            const optimizedQuery = `
                SELECT 
                    t.signature,
                    t.block_time,
                    t.transaction_type,
                    t.sol_spent,
                    t.sol_received,
                    w.address as wallet_address,
                    w.name as wallet_name,
                    w.group_id,
                    g.name as group_name,
                    COALESCE(
                        json_agg(
                            CASE 
                                WHEN tk.mint IS NOT NULL 
                                THEN json_build_object(
                                    'mint', tk.mint,
                                    'symbol', tk.symbol,
                                    'name', tk.name,
                                    'amount', to_.amount,
                                    'decimals', tk.decimals,
                                    'operation_type', to_.operation_type,
                                    'token_price_usd', to_.token_price_usd,
                                    'sol_price_usd', to_.sol_price_usd,
                                    'sol_amount', to_.sol_amount,
                                    'usd_value', to_.usd_value,
                                    'market_cap', to_.market_cap,
                                    'deployment_time', to_.deployment_time
                                )
                                ELSE NULL
                            END
                        ) FILTER (WHERE tk.mint IS NOT NULL),
                        '[]'::json
                    ) as tokens
                FROM transactions t
                JOIN wallets w ON t.wallet_id = w.id
                LEFT JOIN groups g ON w.group_id = g.id
                LEFT JOIN token_operations to_ ON t.id = to_.transaction_id
                LEFT JOIN tokens tk ON to_.token_id = tk.id
                WHERE t.block_time >= NOW() - INTERVAL '${hours} hours'
                ${typeFilter}
                GROUP BY t.id, t.signature, t.block_time, t.transaction_type, 
                         t.sol_spent, t.sol_received, w.address, w.name, 
                         w.group_id, g.name
                ORDER BY t.block_time DESC
                LIMIT $1
            `;

            const result = await this.pool.query(optimizedQuery, queryParams);
            const duration = Date.now() - startTime;

            console.log(`[${new Date().toISOString()}] ⚡ Global transactions fetch completed in ${duration}ms: ${result.rows.length} transactions`);

            return result.rows.map(row => {
                const tokens = Array.isArray(row.tokens) ? row.tokens.filter(t => t !== null) : [];
                
                return {
                    signature: row.signature,
                    time: row.block_time,
                    transactionType: row.transaction_type,
                    solSpent: row.sol_spent ? Number(row.sol_spent).toFixed(6) : null,
                    solReceived: row.sol_received ? Number(row.sol_received).toFixed(6) : null,
                    wallet: {
                        address: row.wallet_address,
                        name: row.wallet_name,
                        group_id: row.group_id,
                        group_name: row.group_name,
                    },
                    tokensBought: tokens.filter(t => t.operation_type === 'buy').map(t => ({
                        mint: t.mint,
                        symbol: t.symbol,
                        name: t.name,
                        amount: Number(t.amount),
                        decimals: t.decimals,
                        token_price_usd: Number(t.token_price_usd) || 0,
                        sol_price_usd: Number(t.sol_price_usd) || 0,
                        sol_amount: Number(t.sol_amount) || 0,
                        usd_value: Number(t.usd_value) || 0,
                        market_cap: Number(t.market_cap) || 0,
                        deployment_time: t.deployment_time
                    })),
                    tokensSold: tokens.filter(t => t.operation_type === 'sell').map(t => ({
                        mint: t.mint,
                        symbol: t.symbol,
                        name: t.name,
                        amount: Number(t.amount),
                        decimals: t.decimals,
                        token_price_usd: Number(t.token_price_usd) || 0,
                        sol_price_usd: Number(t.sol_price_usd) || 0,
                        sol_amount: Number(t.sol_amount) || 0,
                        usd_value: Number(t.usd_value) || 0,
                        market_cap: Number(t.market_cap) || 0,
                        deployment_time: t.deployment_time
                    }))
                };
            });

        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error in global transactions fetch:`, error);
            throw error;
        }
    }

    async getMonitoringStatus(groupId = null) {
        try {
            
            let query = `
                SELECT 
                    COUNT(DISTINCT w.id) as active_wallets,
                    COUNT(CASE WHEN t.transaction_type = 'buy' AND t.block_time >= CURRENT_DATE THEN 1 END) as buy_transactions_today,
                    COUNT(CASE WHEN t.transaction_type = 'sell' AND t.block_time >= CURRENT_DATE THEN 1 END) as sell_transactions_today,
                    COALESCE(SUM(CASE WHEN t.block_time >= CURRENT_DATE THEN t.sol_spent ELSE 0 END), 0) as sol_spent_today,
                    COALESCE(SUM(CASE WHEN t.block_time >= CURRENT_DATE THEN t.sol_received ELSE 0 END), 0) as sol_received_today,
                    COUNT(DISTINCT CASE WHEN t.block_time >= CURRENT_DATE THEN to_.token_id END) as unique_tokens_today
                FROM wallets w
                LEFT JOIN transactions t ON w.id = t.wallet_id 
                LEFT JOIN token_operations to_ ON t.id = to_.transaction_id
                WHERE w.is_active = TRUE
            `;
            
            const params = [];
            
            if (groupId) {
                query += ` AND w.group_id = $1`;
                params.push(groupId);
            }
            
            const result = await this.pool.query(query, params);
            return result.rows[0];
            
        } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error in global monitoring status:`, error);
            throw error;
        }
    }

    async getWalletByAddress(address) {
        const query = `
            SELECT w.*, g.name as group_name, u.username as added_by_username
            FROM wallets w
            LEFT JOIN groups g ON w.group_id = g.id
            LEFT JOIN users u ON w.added_by = u.id
            WHERE w.address = $1
        `;
        const result = await this.pool.query(query, [address]);
        return result.rows[0] || null;
    }

    async getWalletStats(walletId) {
        try {
            const query = `
                SELECT 
                    COUNT(CASE WHEN transaction_type = 'buy' THEN 1 END) as total_buy_transactions,
                    COUNT(CASE WHEN transaction_type = 'sell' THEN 1 END) as total_sell_transactions,
                    COALESCE(SUM(sol_spent), 0) as total_sol_spent,
                    COALESCE(SUM(sol_received), 0) as total_sol_received,
                    MAX(block_time) as last_transaction_at,
                    COUNT(DISTINCT CASE WHEN to_.operation_type = 'buy' THEN to_.token_id END) as unique_tokens_bought,
                    COUNT(DISTINCT CASE WHEN to_.operation_type = 'sell' THEN to_.token_id END) as unique_tokens_sold
                FROM transactions t
                LEFT JOIN token_operations to_ ON t.id = to_.transaction_id
                WHERE t.wallet_id = $1
            `;
            const result = await this.pool.query(query, [walletId]);
            return result.rows[0];
        } catch (error) {
            console.error('❌ Error in getWalletStats:', error);
            throw error;
        }
    }

    async withTransaction(callback) {
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');
            const result = await callback(client);
            await client.query('COMMIT');
            return result;
        } catch (error) {
            await client.query('ROLLBACK');
            throw error;
        } finally {
            client.release();
        }
    }

    async close() {
        try {
            await this.pool.end();
            if (this.priceService) {
                await this.priceService.close();
            }
            console.log('✅ Database connection pool closed');
        } catch (error) {
            console.error('❌ Error closing database pool:', error.message);
        }
    }

    async healthCheck() {
        try {
            const result = await this.pool.query('SELECT NOW() as current_time');
            return {
                status: 'healthy',
                timestamp: result.rows[0].current_time,
                connections: {
                    total: this.pool.totalCount,
                    idle: this.pool.idleCount,
                    waiting: this.pool.waitingCount
                }
            };
        } catch (error) {
            return {
                status: 'unhealthy',
                error: error.message
            };
        }
    }
}

module.exports = Database;