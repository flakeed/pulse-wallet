import React, { useState, useCallback, useEffect } from 'react';

function WalletManager({ onAddWalletsBulk, onCreateGroup, onRemoveAllWallets, groups, selectedGroup, selectedGroupInfo, walletCount }) {
  const [isExpanded, setIsExpanded] = useState(false);
  const [groupId, setGroupId] = useState('');
  const [newGroupName, setNewGroupName] = useState('');
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState(null);
  const [bulkText, setBulkText] = useState('');
  const [bulkLoading, setBulkLoading] = useState(false);
  const [bulkResults, setBulkResults] = useState(null);
  const [bulkValidation, setBulkValidation] = useState(null);
  const [showProgress, setShowProgress] = useState(false);
  const [importProgress, setImportProgress] = useState({ 
    current: 0, 
    total: 0, 
    batch: 0, 
    phase: 'preparing'
  });

  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [deleteLoading, setDeleteLoading] = useState(false);
  const [deleteResults, setDeleteResults] = useState(null);

  const handleCreateGroup = async (e) => {
    e.preventDefault();
    if (!newGroupName.trim()) return;
    try {
      setLoading(true);
      await onCreateGroup(newGroupName.trim());
      setNewGroupName('');
      setMessage({ type: 'success', text: 'Group created successfully' });
    } catch (error) {
      setMessage({ type: 'error', text: error.message });
    } finally {
      setLoading(false);
    }
  };

  const parseBulkInput = useCallback((text) => {
    const lines = text.trim().split('\n');
    const wallets = [];
    const errors = [];
    const seenAddresses = new Set();

    for (let i = 0; i < lines.length; i++) {
      const lineNum = i + 1;
      const trimmedLine = lines[i].trim();

      if (!trimmedLine || trimmedLine.startsWith('#')) continue;

      let address, name;

      if (trimmedLine.includes(',') || trimmedLine.includes('\t')) {
        const parts = trimmedLine.split(/[,\t]/).map(p => p.trim());
        address = parts[0];
        name = parts[1] || null;
      } else {
        address = trimmedLine;
        name = null;
      }

      if (!address) {
        errors.push(`Line ${lineNum}: Empty address`);
        continue;
      }

      if (address.length < 32 || address.length > 44 || !/^[1-9A-HJ-NP-Za-km-z]+$/.test(address)) {
        errors.push(`Line ${lineNum}: Invalid address format`);
        continue;
      }

      if (seenAddresses.has(address)) {
        errors.push(`Line ${lineNum}: Duplicate address - ${address.substring(0, 20)}...`);
        continue;
      }

      seenAddresses.add(address);
      wallets.push({ address, name });
    }

    return { wallets, errors };
  }, []);

  const validateBulkText = useCallback(async (text) => {
    if (!text.trim()) {
      setBulkValidation(null);
      return;
    }

    const { wallets, errors } = parseBulkInput(text);

    setBulkValidation({
      totalLines: text.trim().split('\n').length,
      totalWallets: wallets.length,
      validWallets: wallets.length,
      errors: errors.length,
      canImport: wallets.length > 0 && wallets.length <= 100000,
      tooMany: wallets.length > 100000,
      errorMessages: errors.slice(0, 10)
    });
  }, [parseBulkInput]);

  const handleBulkTextChange = (e) => {
    const newText = e.target.value;
    setBulkText(newText);

    clearTimeout(window.bulkValidationTimeout);
    window.bulkValidationTimeout = setTimeout(() => {
      validateBulkText(newText);
    }, 300);
  };

  const handleBulkSubmit = async (e) => {
    e.preventDefault();

    if (!bulkText.trim()) {
      setBulkResults({ type: 'error', message: 'Please enter wallet addresses' });
      return;
    }

    const { wallets, errors: parseErrors } = parseBulkInput(bulkText);

    if (parseErrors.length > 0 && wallets.length === 0) {
      setBulkResults({
        type: 'error',
        message: `Found ${parseErrors.length} parsing errors and no valid wallets.`,
        details: {
          total: parseErrors.length,
          successful: 0,
          failed: parseErrors.length,
          errors: parseErrors.map(err => ({ address: 'parse_error', error: err }))
        }
      });
      return;
    }

    if (wallets.length === 0) {
      setBulkResults({ type: 'error', message: 'No valid wallet addresses found.' });
      return;
    }

    if (wallets.length > 100000) {
      setBulkResults({ type: 'error', message: 'Maximum 100,000 wallets allowed per bulk import.' });
      return;
    }

    setBulkLoading(true);
    setShowProgress(true);
    setBulkResults(null);
    
    const startTime = Date.now();
    setImportProgress({ 
      current: 0, 
      total: wallets.length, 
      batch: 0, 
      phase: 'starting'
    });

    try {
      const result = await onAddWalletsBulk(wallets, groupId || null, (progress) => {
        const elapsed = (Date.now() - startTime) / 1000;
        const speed = progress.current / elapsed;
        const remaining = progress.total - progress.current;
        const timeRemaining = remaining > 0 ? remaining / speed : 0;

        setImportProgress({
          ...progress,
          speed: Math.round(speed * 100) / 100,
          timeRemaining: Math.round(timeRemaining)
        });
      });

      if (parseErrors.length > 0) {
        result.results.errors.unshift(...parseErrors.map(err => ({ address: 'parse_error', error: err })));
        result.results.failed += parseErrors.length;
      }

      const finalTime = (Date.now() - startTime) / 1000;
      const finalSpeed = result.results.successful / finalTime;

      setBulkResults({
        type: result.results.failed > 0 ? 'warning' : 'success',
        message: `${result.message} (${finalTime.toFixed(1)}s, ${finalSpeed.toFixed(1)})`,
        details: result.results
      });

      if (result.results.successful > 0) {
        setBulkText('');
        setGroupId('');
        setBulkValidation(null);
      }

    } catch (error) {
      console.error('Bulk import failed:', error);
      setBulkResults({
        type: 'error',
        message: `Bulk import failed: ${error.message}`
      });
    } finally {
      setBulkLoading(false);
      setShowProgress(false);
      setImportProgress({ 
        current: 0, 
        total: 0, 
        batch: 0, 
        phase: 'completed'
      });
    }
  };

  const handleDeleteAllWallets = async () => {
    if (!window.confirm(
      `⚠️ DELETE ALL WALLETS?\n\n` +
      `This will permanently remove:\n` +
      `${selectedGroupInfo ? 
        `• ${selectedGroupInfo.walletCount} wallets from "${selectedGroupInfo.groupName}" group\n` :
        `• ${walletCount} wallets from ALL groups\n`
      }` +
      `• All associated transactions\n` +
      `• All token operations\n` +
      `• Unsubscribe from Solana node\n\n` +
      `This action CANNOT be undone!\n\n` +
      `Type "DELETE" in the next prompt to confirm.`
    )) {
      return;
    }

    const confirmText = prompt(
      `Please type "DELETE" to confirm the permanent deletion of ${
        selectedGroupInfo ? 
          `${selectedGroupInfo.walletCount} wallets from "${selectedGroupInfo.groupName}"` :
          `all ${walletCount} wallets`
      }:`
    );

    if (confirmText !== 'DELETE') {
      alert('Deletion cancelled - confirmation text did not match.');
      return;
    }

    setDeleteLoading(true);
    setDeleteResults(null);
    setShowDeleteConfirm(true);

    try {
      
      const result = await onRemoveAllWallets(selectedGroup);
      
      setDeleteResults({
        type: 'success',
        message: result.message,
        details: {
          groupId: selectedGroup,
          groupName: selectedGroupInfo?.groupName,
          previousCount: selectedGroupInfo ? selectedGroupInfo.walletCount : walletCount,
          newCounts: result.newCounts
        }
      });

      setBulkText('');
      setBulkValidation(null);
      setBulkResults(null);
      
    } catch (error) {
      console.error('Delete all wallets failed:', error);
      setDeleteResults({
        type: 'error',
        message: error.message || 'Failed to delete wallets'
      });
    } finally {
      setDeleteLoading(false);
    }
  };

  const clearBulkData = () => {
    setBulkText('');
    setBulkResults(null);
    setBulkValidation(null);
    setDeleteResults(null);
  };

  useEffect(() => {
    return () => {
      if (window.bulkValidationTimeout) {
        clearTimeout(window.bulkValidationTimeout);
      }
    };
  }, []);

  const currentWalletCount = selectedGroupInfo ? selectedGroupInfo.walletCount : walletCount;
  const currentGroupName = selectedGroupInfo ? selectedGroupInfo.groupName : 'All Groups';

  return (
    <div className="bg-gray-800 border-b border-gray-700">
      <div className="px-4 py-2">
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="flex items-center justify-between w-full text-left"
        >
          <div className="flex items-center space-x-2">
            <svg className="w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} 
                d="M12 6v6m0 0v6m0-6h6m-6 0H6" />
            </svg>
            <span className="text-white font-medium">Wallet Management</span>
            {bulkValidation && bulkValidation.validWallets > 0 && (
              <span className="bg-blue-600 text-white text-xs px-2 py-1 rounded">
                {bulkValidation.validWallets} ready
              </span>
            )}
            {currentWalletCount > 0 && (
              <span className="bg-green-600 text-white text-xs px-2 py-1 rounded">
                {currentWalletCount} active
              </span>
            )}
          </div>
          <svg 
            className={`w-4 h-4 text-gray-400 transition-transform ${isExpanded ? 'rotate-180' : ''}`} 
            fill="none" stroke="currentColor" viewBox="0 0 24 24"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        </button>
      </div>

      {isExpanded && (
        <div className="border-t border-gray-700 p-4 space-y-4">
          <div className="bg-gray-700/50 rounded p-3">
            <div className="flex items-center justify-between">
              <div>
                <h4 className="text-white text-sm font-medium">Current Status</h4>
                <p className="text-gray-300 text-sm">
                  Monitoring {currentWalletCount} wallets in {currentGroupName}
                </p>
              </div>
              {currentWalletCount > 0 && (
                <button
                  onClick={handleDeleteAllWallets}
                  disabled={deleteLoading}
                  className="bg-red-600 hover:bg-red-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white text-sm px-4 py-2 rounded transition-colors flex items-center space-x-2"
                  title={`Delete all wallets from ${currentGroupName}`}
                >
                  {deleteLoading ? (
                    <>
                      <div className="animate-spin rounded-full h-3 w-3 border-b-2 border-white"></div>
                      <span>Deleting...</span>
                    </>
                  ) : (
                    <>
                      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                          d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                      </svg>
                      <span>Delete All</span>
                    </>
                  )}
                </button>
              )}
            </div>
          </div>

          {deleteResults && (
            <div className={`p-3 rounded border ${
              deleteResults.type === 'success'
                ? 'bg-green-900/20 border-green-700'
                : 'bg-red-900/20 border-red-700'
            }`}>
              <div className={`text-sm font-medium mb-2 ${
                deleteResults.type === 'success' ? 'text-green-400' : 'text-red-400'
              }`}>
                {deleteResults.type === 'success' ? '✅ Deletion Completed' : '❌ Deletion Failed'}
              </div>
              <div className="text-gray-300 text-sm mb-2">
                {deleteResults.message}
              </div>
              
              {deleteResults.details && deleteResults.type === 'success' && (
                <div className="text-xs text-gray-400 space-y-1">
                  <div>• Previous count: {deleteResults.details.previousCount}</div>
                  {deleteResults.details.groupName && (
                    <div>• Group: {deleteResults.details.groupName}</div>
                  )}
                  <div>• Remaining wallets: {deleteResults.details.newCounts?.totalWallets || 0}</div>
                  <div>• Unsubscribed from Solana node</div>
                  <div>• All associated data removed</div>
                </div>
              )}
            </div>
          )}

          <div>
            <h4 className="text-white text-sm font-medium mb-2">Create New Group</h4>
            <div className="flex space-x-2">
              <input
                type="text"
                value={newGroupName}
                onChange={(e) => setNewGroupName(e.target.value)}
                className="flex-1 bg-gray-700 border border-gray-600 text-white text-sm rounded px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="Enter group name..."
                disabled={loading}
              />
              <button
                type="button"
                onClick={handleCreateGroup}
                disabled={loading || !newGroupName.trim()}
                className="bg-green-600 hover:bg-green-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white text-sm px-4 py-2 rounded transition-colors"
              >
                Create
              </button>
            </div>
          </div>

          <div>
            <h4 className="text-white text-sm font-medium mb-2">Bulk Import</h4>
            
            {bulkValidation && (
              <div className={`mb-3 p-3 rounded border ${
                bulkValidation.canImport
                  ? 'bg-green-900/20 border-green-700'
                  : bulkValidation.tooMany
                    ? 'bg-red-900/20 border-red-700'
                    : 'bg-yellow-900/20 border-yellow-700'
              }`}>
                <div className="flex items-center justify-between mb-2">
                  <span className={`text-sm font-medium ${
                    bulkValidation.canImport ? 'text-green-400' : bulkValidation.tooMany ? 'text-red-400' : 'text-yellow-400'
                  }`}>
                    {bulkValidation.validWallets} valid wallets found
                  </span>
                  {bulkValidation.canImport && (
                    <span className="text-xs bg-green-600 text-green-100 px-2 py-1 rounded">Ready</span>
                  )}
                </div>
                
                {bulkValidation.tooMany && (
                  <div className="text-red-400 text-sm">
                    Too many wallets! Maximum 100,000 allowed.
                  </div>
                )}

                {bulkValidation.errorMessages && bulkValidation.errorMessages.length > 0 && (
                  <div className="text-xs text-gray-400 mt-2">
                    {bulkValidation.errors} errors found
                  </div>
                )}
              </div>
            )}

            {showProgress && bulkLoading && (
              <div className="mb-3 p-3 bg-blue-900/20 border border-blue-700 rounded">
                <div className="flex items-center space-x-3 mb-2">
                  <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-500"></div>
                  <span className="text-blue-400 text-sm">
                    Phase: {importProgress.phase} • {importProgress.current}/{importProgress.total}
                  </span>
                </div>
                {importProgress.total > 0 && (
                  <div className="w-full bg-gray-700 rounded-full h-2">
                    <div
                      className="bg-blue-500 h-2 rounded-full transition-all duration-300"
                      style={{ width: `${Math.min((importProgress.current / importProgress.total) * 100, 100)}%` }}
                    ></div>
                  </div>
                )}
              </div>
            )}

            {bulkResults && (
              <div className={`mb-3 p-3 rounded border ${
                bulkResults.type === 'success'
                  ? 'bg-green-900/20 border-green-700'
                  : bulkResults.type === 'warning'
                    ? 'bg-yellow-900/20 border-yellow-700'
                    : 'bg-red-900/20 border-red-700'
              }`}>
                <div className={`text-sm font-medium mb-2 ${
                  bulkResults.type === 'success' ? 'text-green-400' : 
                  bulkResults.type === 'warning' ? 'text-yellow-400' : 'text-red-400'
                }`}>
                  {bulkResults.message}
                </div>

                {bulkResults.details && (
                  <div className="grid grid-cols-3 gap-4 text-xs">
                    <div className="text-center">
                      <div className="text-lg font-bold text-white">{bulkResults.details.total}</div>
                      <div className="text-gray-400">Total</div>
                    </div>
                    <div className="text-center">
                      <div className="text-lg font-bold text-green-400">{bulkResults.details.successful}</div>
                      <div className="text-gray-400">Success</div>
                    </div>
                    <div className="text-center">
                      <div className="text-lg font-bold text-red-400">{bulkResults.details.failed}</div>
                      <div className="text-gray-400">Failed</div>
                    </div>
                  </div>
                )}
              </div>
            )}

            <form onSubmit={handleBulkSubmit} className="space-y-3">
              <div className="flex justify-between items-center">
                <label className="text-gray-300 text-sm">Wallet Addresses</label>
                <button
                  type="button"
                  onClick={clearBulkData}
                  className="text-xs text-gray-500 hover:text-gray-300"
                >
                  Clear
                </button>
              </div>
              <textarea
                value={bulkText}
                onChange={handleBulkTextChange}
                className="w-full bg-gray-700 border border-gray-600 text-white text-sm rounded px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 font-mono"
                placeholder="9yuiiicyZ2McJkFz7v7GvPPPXX92RX4jXDSdvhF5BkVd,Trading Wallet 1
53nHsQXkzZUp5MF1BK6Qoa48ud3aXfDFJBbe1oECPucC
Cupjy3x8wfwCcLMkv5SqPtRjsJd5Zk8q7X2NGNGJGi5y,Important Wallet"
                rows={6}
                disabled={bulkLoading}
              />

              <div className="flex space-x-2">
                <select
                  value={groupId}
                  onChange={(e) => setGroupId(e.target.value)}
                  className="flex-1 bg-gray-700 border border-gray-600 text-white text-sm rounded px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  disabled={bulkLoading}
                >
                  <option value="">Select group (optional)</option>
                  {groups.map((group) => (
                    <option key={group.id} value={group.id}>
                      {group.name} ({group.wallet_count} wallets)
                    </option>
                  ))}
                </select>

                <button
                  type="submit"
                  disabled={bulkLoading || !bulkText.trim() || (bulkValidation && !bulkValidation.canImport)}
                  className="bg-blue-600 hover:bg-blue-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white text-sm px-6 py-2 rounded transition-colors flex items-center"
                >
                  {bulkLoading ? (
                    <>
                      <div className="animate-spin rounded-full h-3 w-3 border-b-2 border-white mr-2"></div>
                      Importing...
                    </>
                  ) : (
                    <>🚀 Import{bulkValidation && bulkValidation.validWallets > 0 ? ` ${bulkValidation.validWallets}` : ''}</>
                  )}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}

export default WalletManager;