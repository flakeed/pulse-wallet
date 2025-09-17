module.exports = (auth, db, solanaGrpcService) => {
  const express = require('express');
  const router = express.Router();
  const { redis } = require('../services/tokenService'); 

  router.get('/', auth.authRequired, async (req, res) => {
    try {
      const groups = await db.getGroups();
      res.json(groups);
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Error fetching groups:`, error);
      res.status(500).json({ error: 'Failed to fetch groups' });
    }
  });

  router.post('/', auth.authRequired, async (req, res) => {
    try {
      const { name } = req.body;
      const createdBy = req.user.id;
      
      if (!name || name.trim().length === 0) {
        return res.status(400).json({ error: 'Group name is required' });
      }
      
      const group = await db.addGroup(name.trim(), createdBy);
      res.json({
        success: true,
        group,
        message: 'Global group created successfully',
      });
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Error creating group:`, error);
      if (error.message.includes('already exists')) {
        res.status(409).json({ error: 'Group name already exists globally' });
      } else {
        res.status(500).json({ error: 'Failed to create group' });
      }
    }
  });

  router.post('/switch', auth.authRequired, async (req, res) => {
    try {
      const { groupId } = req.body;
      const userId = req.user.id;
      
      console.log(`[${new Date().toISOString()}] 🔄 Group switch requested: user ${userId} -> ${groupId || 'all'}`);
      
      if (groupId) {
        const query = `SELECT id FROM groups WHERE id = $1`;
        const result = await db.pool.query(query, [groupId]);
        if (result.rows.length === 0) {
          return res.status(404).json({ error: 'Group not found' });
        }
      }
      
      await solanaGrpcService.switchGroup(groupId);
      
      const refreshMessage = JSON.stringify({
        type: 'GROUP_SWITCH',
        groupId: groupId || null,
        timestamp: Date.now(),
        message: 'Group context changed, please refresh'
      });
      
      const channels = ['transactions'];
      if (groupId) {
        channels.push(`transactions:group:${groupId}`);
      }
      
      const pipeline = redis.pipeline();
      channels.forEach(channel => {
        pipeline.publish(channel, refreshMessage);
      });
      await pipeline.exec();
      
      console.log(`[${new Date().toISOString()}] ✅ Group switch completed and clients notified`);
      
      res.json({
        success: true,
        message: `Switched to group ${groupId || 'all'}`,
        groupId: groupId || null,
        actionRequired: 'refresh'
      });
      
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Error switching group:`, error);
      res.status(500).json({ error: 'Failed to switch group' });
    }
  });

  return router;
};