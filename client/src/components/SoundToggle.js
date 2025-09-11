import React, { useState, useEffect } from 'react';
import soundManager from '../utils/soundManager';

function SoundToggle() {
  const [isEnabled, setIsEnabled] = useState(false);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    setIsEnabled(soundManager.getEnabled());
  }, []);

  const handleToggle = async () => {
    setIsLoading(true);
    
    try {
      const newState = !isEnabled;
      soundManager.setEnabled(newState);
      setIsEnabled(newState);
      
      if (newState) {
        setTimeout(() => {
          soundManager.testSound();
        }, 100);
      }
    } catch (error) {
      console.error('Failed to toggle sound:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleTestSound = async () => {
    if (!isEnabled) return;
    
    setIsLoading(true);
    try {
      await soundManager.testSound();
    } catch (error) {
      console.error('Failed to test sound:', error);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="flex items-center space-x-2">
      <button
        onClick={handleToggle}
        disabled={isLoading}
        className={`flex items-center space-x-2 px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
          isEnabled
            ? 'bg-green-600 hover:bg-green-700 text-white'
            : 'bg-gray-600 hover:bg-gray-700 text-gray-300'
        } ${isLoading ? 'opacity-50 cursor-not-allowed' : ''}`}
        title={isEnabled ? 'Disable new token sounds' : 'Enable new token sounds'}
      >
        {isLoading ? (
          <div className="animate-spin rounded-full h-3 w-3 border border-white border-t-transparent"></div>
        ) : (
          <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
            {isEnabled ? (
              <path fillRule="evenodd" d="M9.383 3.076A1 1 0 0110 4v12a1 1 0 01-1.617.82L4.29 13.372a1 1 0 00-.536-.153H2a1 1 0 01-1-1V8.78a1 1 0 011-1h1.754a1 1 0 00.536-.153l4.093-3.448a1 1 0 011.617.82zM14.657 2.929a1 1 0 011.414 0A9.972 9.972 0 0119 10a9.972 9.972 0 01-2.929 7.071 1 1 0 01-1.414-1.414A7.971 7.971 0 0017 10c0-2.21-.894-4.208-2.343-5.657a1 1 0 010-1.414zm-2.829 2.828a1 1 0 011.415 0A5.983 5.983 0 0115 10a5.984 5.984 0 01-1.757 4.243 1 1 0 01-1.415-1.415A3.984 3.984 0 0013 10a3.983 3.983 0 00-1.172-2.828 1 1 0 010-1.415z" clipRule="evenodd" />
            ) : (
              <path fillRule="evenodd" d="M9.383 3.076A1 1 0 0110 4v12a1 1 0 01-1.617.82L4.29 13.372a1 1 0 00-.536-.153H2a1 1 0 01-1-1V8.78a1 1 0 011-1h1.754a1 1 0 00.536-.153l4.093-3.448a1 1 0 011.617.82zM12.293 7.293a1 1 0 011.414 0L15 8.586l1.293-1.293a1 1 0 111.414 1.414L16.414 10l1.293 1.293a1 1 0 01-1.414 1.414L15 11.414l-1.293 1.293a1 1 0 01-1.414-1.414L13.586 10l-1.293-1.293a1 1 0 010-1.414z" clipRule="evenodd" />
            )}
          </svg>
        )}
        <span>{isEnabled ? 'Sound On' : 'Sound Off'}</span>
      </button>
      
      {isEnabled && (
        <button
          onClick={handleTestSound}
          disabled={isLoading}
          className="px-2 py-1.5 text-xs text-gray-400 hover:text-white hover:bg-gray-700 rounded transition-colors"
          title="Test sound"
        >
          <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14.828 14.828a4 4 0 01-5.656 0M9 10h1.586a1 1 0 01.707.293l2.414 2.414a1 1 0 00.707.293H15M9 10v4a1 1 0 001 1h4M9 10V9a1 1 0 011-1h4a1 1 0 011 1v1M9 10H8a1 1 0 00-1 1v3a1 1 0 001 1h1m0-5l4 4m0 0v-3a1 1 0 00-1-1H9" />
          </svg>
        </button>
      )}
    </div>
  );
}

export default SoundToggle;