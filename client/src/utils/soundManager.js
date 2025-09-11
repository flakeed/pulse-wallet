class SoundManager {
    constructor() {
      this.isEnabled = localStorage.getItem('newTokenSoundEnabled') === 'true';
      this.audioContext = null;
      this.soundBuffer = null;
      this.isInitialized = false;
      
      this.notification = {
        frequency1: 800,  
        frequency2: 1200, 
        duration: 0.3,
        fadeOut: 0.1
      };
    }
  
    async initialize() {
      if (this.isInitialized) return;
      
      try {
        this.audioContext = new (window.AudioContext || window.webkitAudioContext)();
        
        await this.generateNotificationSound();
        
        this.isInitialized = true;
        console.log('🔊 Sound manager initialized');
      } catch (error) {
        console.error('❌ Failed to initialize sound manager:', error);
      }
    }
  
    async generateNotificationSound() {
      if (!this.audioContext) return;
  
      const sampleRate = this.audioContext.sampleRate;
      const duration = this.notification.duration;
      const fadeOutDuration = this.notification.fadeOut;
      const length = sampleRate * duration;
      
      const buffer = this.audioContext.createBuffer(1, length, sampleRate);
      const data = buffer.getChannelData(0);
  
      for (let i = 0; i < length; i++) {
        const time = i / sampleRate;
        const fadeOutTime = duration - fadeOutDuration;
        
        let sample = 0;
        
        const freq1 = this.notification.frequency1 * (1 + Math.sin(time * 15) * 0.1);
        sample += Math.sin(2 * Math.PI * freq1 * time) * 0.4;
        
        const freq2 = this.notification.frequency2 * (1 + Math.sin(time * 8) * 0.05);
        sample += Math.sin(2 * Math.PI * freq2 * time) * 0.2;
        
        if (sample > 0.3) sample = 0.3 + (sample - 0.3) * 0.5;
        if (sample < -0.3) sample = -0.3 + (sample + 0.3) * 0.5;
        
        let envelope = 1;
        if (time > fadeOutTime) {
          envelope = 1 - (time - fadeOutTime) / fadeOutDuration;
        }
        
        if (time < 0.02) {
          envelope *= time / 0.02;
        }
        
        data[i] = sample * envelope * 0.3; 
      }
  
      this.soundBuffer = buffer;
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