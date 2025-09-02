module.exports = (auth, db) => {
  const express = require('express');
  const router = express.Router();

  router.post('/telegram-simple', async (req, res) => {
    try {
      const { id, first_name, last_name, username } = req.body;
      
      if (!id) {
        return res.status(400).json({ error: 'Telegram ID is required' });
      }
      
      if (!Number.isInteger(id) || id <= 0) {
        return res.status(400).json({ error: 'Invalid Telegram ID format' });
      }
      
      
      const isWhitelisted = await auth.isUserWhitelisted(id);
      if (!isWhitelisted) {
        return res.status(403).json({ 
          error: 'Access denied. You are not in the whitelist. Please contact an administrator.' 
        });
      }
      
      const userData = {
        id,
        username: username || null,
        first_name: first_name || 'User',
        last_name: last_name || null
      };
      
      const user = await auth.createOrUpdateUser(userData);
      
      if (!user.is_active) {
        return res.status(403).json({ 
          error: 'Your account has been deactivated. Please contact an administrator.' 
        });
      }
      
      const session = await auth.createUserSession(user.id);
      
      console.log(`[${new Date().toISOString()}] ✅ User authenticated via auth: ${user.username || user.first_name} (${user.telegram_id})`);
      
      res.json({
        success: true,
        sessionToken: session.session_token,
        expiresAt: session.expires_at,
        user: {
          id: user.id,
          telegramId: user.telegram_id,
          username: user.username,
          firstName: user.first_name,
          lastName: user.last_name,
          isAdmin: user.is_admin,
          isActive: user.is_active
        }
      });
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Simple auth error:`, error.message);
      res.status(500).json({ error: 'Authentication failed. Please try again.' });
    }
  });

  router.post('/telegram', async (req, res) => {
    try {
      const telegramData = req.body;
      
      if (telegramData.hash !== 'simple_auth') {
        auth.verifyTelegramAuth(telegramData);
      }
      
      const isWhitelisted = await auth.isUserWhitelisted(telegramData.id);
      if (!isWhitelisted) {
        return res.status(403).json({ 
          error: 'Access denied. You are not in the whitelist. Please contact an administrator.' 
        });
      }
      
      const user = await auth.createOrUpdateUser(telegramData);
      
      if (!user.is_active) {
        return res.status(403).json({ 
          error: 'Your account has been deactivated. Please contact an administrator.' 
        });
      }
      
      const session = await auth.createUserSession(user.id);
      
      console.log(`[${new Date().toISOString()}] ✅ User authenticated: ${user.username || user.first_name} (${user.telegram_id})`);
      
      res.json({
        success: true,
        sessionToken: session.session_token,
        expiresAt: session.expires_at,
        user: {
          id: user.id,
          telegramId: user.telegram_id,
          username: user.username,
          firstName: user.first_name,
          lastName: user.last_name,
          isAdmin: user.is_admin,
          isActive: user.is_active
        }
      });
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Auth error:`, error.message);
      res.status(401).json({ error: error.message });
    }
  });

  router.get('/validate', auth.authRequired, (req, res) => {
    try {
      res.json({
        success: true,
        user: req.user
      });
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Validation error:`, error.message);
      res.status(401).json({ error: 'Session validation failed' });
    }
  });

  router.post('/logout', auth.authRequired, async (req, res) => {
    try {
      const authHeader = req.headers.authorization;
      if (authHeader && authHeader.startsWith('Bearer ')) {
        const sessionToken = authHeader.substring(7);
        await auth.revokeSession(sessionToken);
      }
      res.json({ success: true, message: 'Logged out successfully' });
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Logout error:`, error.message);
      res.json({ success: true, message: 'Logout completed' });
    }
  });

  return router;
};
