module.exports = {
  startWebSocketService: (solanaWebSocketService) => async () => {
    let retries = 0;
    const maxRetries = 5;
    const retryDelay = 5000;

    while (retries < maxRetries) {
      try {
        await solanaWebSocketService.start();
        console.log(`[${new Date().toISOString()}] 🚀 Solana WebSocket service started successfully`);
        return;
      } catch (error) {
        retries++;
        console.error(
          `[${new Date().toISOString()}] ❌ Failed to start WebSocket service (attempt ${retries}/${maxRetries}):`,
          error.message
        );
        if (retries < maxRetries) {
          console.log(`[${new Date().toISOString()}] ⏳ Retrying in ${retryDelay / 1000} seconds...`);
          await new Promise((resolve) => setTimeout(resolve, retryDelay));
        }
      }
    }
    console.error(`[${new Date().toISOString()}] 🛑 Max retries reached. Global WebSocket service failed to start.`);
  }
};
