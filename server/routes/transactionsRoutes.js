module.exports = (auth, db, redis, sseClients) => {
  const express = require('express');
  const router = express.Router();

  router.get('/stream', async (req, res) => {
    try {
      const token = req.query.token || (req.headers.authorization && req.headers.authorization.substring(7));
      if (!token) return res.status(401).json({ error: 'No authentication token provided' });

      const session = await auth.validateSession(token);
      if (!session) return res.status(401).json({ error: 'Invalid or expired session' });

      const groupId = req.query.groupId || null;
      console.log(`[${new Date().toISOString()}] ✅ SSE client authenticated: ${session.user_id}${groupId ? `, group ${groupId}` : ' (global)'}`);

      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Headers', 'Cache-Control');
      res.flushHeaders();

      sseClients.add(res);
      console.log(`[${new Date().toISOString()}] 📊 SSE clients count: ${sseClients.size}`);

      const subscriber = redis.duplicate();

      const channels = ['transactions']; 
      if (groupId) {
        channels.push(`transactions:group:${groupId}`); 
      }

      console.log(`[${new Date().toISOString()}] 🔊 Subscribing to channels:`, channels);

      await Promise.all(channels.map(channel => subscriber.subscribe(channel)));

      const messageHandler = (channel, message) => {
        if (res.writable && !res.destroyed) {
          try {
            const transaction = JSON.parse(message);

            if (groupId && transaction.groupId !== groupId) {
              console.log(`[${new Date().toISOString()}] ⏭️ Filtered transaction ${transaction.signature} (wrong group: ${transaction.groupId} vs ${groupId})`);
              return; 
            }

            console.log(`[${new Date().toISOString()}] 📡 Broadcasting SSE message: ${transaction.signature} for group ${groupId || 'all'}`);
            res.write(`data: ${message}\n\n`);
          } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error parsing SSE message:`, error.message);
          }
        } else {
          console.warn(`[${new Date().toISOString()}] ⚠️ SSE response not writable`);
        }
      };

      subscriber.on('message', messageHandler);

      res.write(`: ping\n\n`);

      const cleanup = () => {
        console.log(`[${new Date().toISOString()}] 🔌 SSE client disconnected`);
        subscriber.off('message', messageHandler);
        subscriber.quit();
        sseClients.delete(res);
        if (!res.destroyed) {
          res.end();
        }
      };

      req.on('close', cleanup);
      req.on('end', cleanup);

      res.on('error', (error) => {
        console.error(`[${new Date().toISOString()}] ❌ SSE response error:`, error.message);
        cleanup();
      });

      const keepAliveInterval = setInterval(() => {
        if (res.writable && !res.destroyed) {
          res.write(`: keep-alive\n\n`);
        } else {
          clearInterval(keepAliveInterval);
        }
      }, 30000);

      req.on('close', () => clearInterval(keepAliveInterval));

    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ SSE setup error:`, error.message);
      if (!res.headersSent) {
        res.status(500).json({ error: 'Failed to setup SSE connection' });
      }
    }
  });

  return router;
};