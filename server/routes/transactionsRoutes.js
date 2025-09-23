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
  
      const subscriber = redis.duplicate();
      
      const channels = ['transactions']; 
      if (groupId) {
        channels.push(`transactions:group:${groupId}`); 
      }
      
      await Promise.all(channels.map(channel => subscriber.subscribe(channel)));
  
      const messageHandler = (channel, message) => {
        if (res.writable) {
          try {
            const transaction = JSON.parse(message);
            
            if (groupId && transaction.groupId !== groupId) {
              return; 
            }
            
            console.log(`[${new Date().toISOString()}] 📡 Broadcasting SSE message: ${transaction.signature} for group ${groupId || 'all'}`);
            res.write(`data: ${message}\n\n`);
          } catch (error) {
            console.error(`[${new Date().toISOString()}] ❌ Error parsing SSE message:`, error.message);
          }
        }
      };
  
      subscriber.on('message', messageHandler);
  
      req.on('close', () => {
        console.log(`[${new Date().toISOString()}] 🔌 SSE client disconnected`);
        subscriber.off('message', messageHandler);
        subscriber.quit();
        sseClients.delete(res);
        res.end();
      });
  
      const keepAlive = setInterval(() => {
        if (res.writable) res.write(': keep-alive\n\n');
        else clearInterval(keepAlive);
      }, 30000);
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ SSE setup error:`, error.message);
      res.status(500).json({ error: 'Failed to setup SSE connection' });
    }
  });

  return router;
};