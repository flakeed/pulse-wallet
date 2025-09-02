module.exports = (auth, db) => {
  const express = require('express');
  const router = express.Router();

  router.get('/whitelist', auth.authRequired, auth.adminRequired, async (req, res) => {
    try {
      const whitelist = await auth.getWhitelist();
      res.json(whitelist);
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Error fetching whitelist:`, error);
      res.status(500).json({ error: 'Failed to fetch whitelist' });
    }
  });

  router.post('/whitelist', auth.authRequired, auth.adminRequired, async (req, res) => {
    try {
      const { telegramId, notes } = req.body;
      
      if (!telegramId) {
        return res.status(400).json({ error: 'Telegram ID is required' });
      }
      
      const result = await auth.addToWhitelist(telegramId, req.user.id, notes);
      res.json({ success: true, message: 'User added to whitelist', result });
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Error adding to whitelist:`, error);
      res.status(500).json({ error: 'Failed to add user to whitelist' });
    }
  });

  router.delete('/whitelist/:telegramId', auth.authRequired, auth.adminRequired, async (req, res) => {
    try {
      const { telegramId } = req.params;
      await auth.removeFromWhitelist(telegramId);
      res.json({ success: true, message: 'User removed from whitelist' });
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Error removing from whitelist:`, error);
      res.status(500).json({ error: 'Failed to remove user from whitelist' });
    }
  });

  router.get('/users', auth.authRequired, auth.adminRequired, async (req, res) => {
    try {
      const query = `
        SELECT id, telegram_id, username, first_name, last_name, 
               is_active, is_admin, created_at, last_login
        FROM users 
        ORDER BY created_at DESC
      `;
      const result = await db.pool.query(query);
      res.json(result.rows);
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Error fetching users:`, error);
      res.status(500).json({ error: 'Failed to fetch users' });
    }
  });

  router.patch('/users/:userId/status', auth.authRequired, auth.adminRequired, async (req, res) => {
    try {
      const { userId } = req.params;
      const { isActive } = req.body;
      
      const query = `
        UPDATE users 
        SET is_active = $1, updated_at = CURRENT_TIMESTAMP 
        WHERE id = $2 
        RETURNING *
      `;
      const result = await db.pool.query(query, [isActive, userId]);
      
      if (result.rows.length === 0) {
        return res.status(404).json({ error: 'User not found' });
      }
      
      res.json({ success: true, message: 'User status updated', user: result.rows[0] });
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Error updating user status:`, error);
      res.status(500).json({ error: 'Failed to update user status' });
    }
  });

  router.patch('/users/:userId/admin', auth.authRequired, auth.adminRequired, async (req, res) => {
    try {
      const { userId } = req.params;
      const { isAdmin } = req.body;
      
      if (userId === req.user.id && !isAdmin) {
        return res.status(400).json({ error: 'You cannot remove admin privileges from yourself' });
      }
      
      const query = `
        UPDATE users 
        SET is_admin = $1, updated_at = CURRENT_TIMESTAMP 
        WHERE id = $2 
        RETURNING *
      `;
      const result = await db.pool.query(query, [isAdmin, userId]);
      
      if (result.rows.length === 0) {
        return res.status(404).json({ error: 'User not found' });
      }
      
      res.json({ success: true, message: 'Admin status updated', user: result.rows[0] });
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Error updating admin status:`, error);
      res.status(500).json({ error: 'Failed to update admin status' });
    }
  });

  router.get('/stats', auth.authRequired, auth.adminRequired, async (req, res) => {
    try {
      const query = `
        SELECT 
          (SELECT COUNT(*) FROM users) as total_users,
          (SELECT COUNT(*) FROM users WHERE is_active = true) as active_users,
          (SELECT COUNT(*) FROM wallets) as total_wallets,
          (SELECT COUNT(*) FROM transactions) as total_transactions,
          (SELECT COUNT(*) FROM groups) as total_groups,
          (SELECT COUNT(*) FROM whitelist) as whitelist_size,
          (SELECT COALESCE(SUM(sol_spent), 0) FROM transactions) as total_sol_spent,
          (SELECT COALESCE(SUM(sol_received), 0) FROM transactions) as total_sol_received
      `;
      const result = await db.pool.query(query);
      res.json(result.rows[0]);
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Error fetching admin stats:`, error);
      res.status(500).json({ error: 'Failed to fetch statistics' });
    }
  });

  return router;
};
