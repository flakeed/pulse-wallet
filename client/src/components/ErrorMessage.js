import React from 'react';

function ErrorMessage({ error }) {
  return (
    <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-6">
      <span className="text-red-700">{error}</span>
    </div>
  );
}

export default ErrorMessage;