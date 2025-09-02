import React from 'react';

function LoadingSpinner() {
  return (
    <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-6">
      <div className="flex items-center">
        <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-600 mr-3"></div>
        <span className="text-blue-700">Loading...</span>
      </div>
    </div>
  );
}

export default LoadingSpinner;