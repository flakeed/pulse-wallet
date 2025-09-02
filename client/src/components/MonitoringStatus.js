import React from 'react';

function MonitoringStatus({ status, onToggle }) {
  const { isMonitoring, processedSignatures } = status;

  return (
    <div className="bg-gray-900 border-b border-gray-700 px-4 py-2">
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <div className="flex items-center space-x-2">
            <div className={`w-2 h-2 rounded-full ${isMonitoring ? 'bg-green-500 animate-pulse' : 'bg-red-500'}`}></div>
            <span className="text-white text-sm font-medium">
              {isMonitoring ? 'Monitoring Active' : 'Monitoring Inactive'}
            </span>
          </div>
          <div className="text-gray-400 text-xs hidden sm:block">
            {isMonitoring ? '🔍 Tracking wallet activities' : '⏸️ Click Start to begin monitoring'}
          </div>
        </div>

        <div className="flex items-center space-x-2">
          <button
            onClick={() => onToggle('start')}
            disabled={isMonitoring}
            className="px-3 py-1 bg-green-600 hover:bg-green-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white text-sm rounded transition-colors"
          >
            Start
          </button>
          <button
            onClick={() => onToggle('stop')}
            disabled={!isMonitoring}
            className="px-3 py-1 bg-red-600 hover:bg-red-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white text-sm rounded transition-colors"
          >
            Stop
          </button>
        </div>
      </div>
    </div>
  );
}

export default MonitoringStatus;