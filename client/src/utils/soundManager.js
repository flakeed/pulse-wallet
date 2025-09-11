class SoundManager {
    constructor() {
      this.isEnabled = localStorage.getItem('newTokenSoundEnabled') === 'true';
      this.audioContext = null;
      this.soundBuffer = null;
      this.isInitialized = false;
      
      this.notificationSoundUrl = './client/public/Voicy_Notifications.mp3';
    }
  
    async initialize() {
      if (this.isInitialized) return;
      
      try {
        this.audioContext = new (window.AudioContext || window.webkitAudioContext)();
        
        await this.loadNotificationSound();
        
        this.isInitialized = true;
        console.log('🔊 Sound manager initialized');
      } catch (error) {
        console.error('❌ Failed to initialize sound manager:', error);
      }
    }
  
    async loadNotificationSound() {
      if (!this.audioContext) return;
  
      try {
        const response = await fetch(this.notificationSoundUrl);
        if (!response.ok) throw new Error('Failed to fetch MP3 file');
        
        const arrayBuffer = await response.arrayBuffer();
        this.soundBuffer = await this.audioContext.decodeAudioData(arrayBuffer);
        console.log('🔊 Notification sound loaded');
      } catch (error) {
        console.error('❌ Failed to load MP3 sound:', error);
      }
    }
  
    async playNewTokenSound() {
      if (!this.isEnabled || !this.isInitialized || !this.audioContext || !this.soundBuffer) {
        return;
      }
  
      try {
        if (this.audioContext.state === 'suspended') {
          await this.audioContext.resume();
        }
  
        const source = this.audioContext.createBufferSource();
        const gainNode = this.audioContext.createGain();
        
        source.buffer = this.soundBuffer;
        source.connect(gainNode);
        gainNode.connect(this.audioContext.destination);
        
        gainNode.gain.value = 0.2; 
        
        source.start();
        
        console.log('🔊 New token notification sound played');
      } catch (error) {
        console.error('❌ Failed to play notification sound:', error);
      }
    }
  
    setEnabled(enabled) {
      this.isEnabled = enabled;
      localStorage.setItem('newTokenSoundEnabled', enabled.toString());
      
      if (enabled && !this.isInitialized) {
        this.initialize();
      }
      
      console.log(`🔊 Token notification sound ${enabled ? 'enabled' : 'disabled'}`);
    }
  
    getEnabled() {
      return this.isEnabled;
    }
  
    async testSound() {
      if (!this.isInitialized) {
        await this.initialize();
      }
      await this.playNewTokenSound();
    }
}

const soundManager = new SoundManager();

export default soundManager;