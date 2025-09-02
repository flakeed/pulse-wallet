module.exports = {
  startSessionCleaner: (auth) => {
    setInterval(async () => {
      try {
        const cleaned = await auth.cleanExpiredSessions();
        if (cleaned > 0) {
          console.log(`[${new Date().toISOString()}] 🧹 Cleaned ${cleaned} expired sessions`);
        }
      } catch (error) {
        console.error(`[${new Date().toISOString()}] ❌ Error cleaning sessions:`, error);
      }
    }, 60 * 60 * 1000);
  }
};
