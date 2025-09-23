import React, { useState, useEffect, useRef } from 'react';
import Header from './components/Header';
import WalletManager from './components/WalletManager';
import MonitoringStatus from './components/MonitoringStatus';
import LoadingSpinner from './components/LoadingSpinner';
import ErrorMessage from './components/ErrorMessage';
import TokenTracker from './components/TokenTracker';
import TelegramLogin from './components/TelegramLogin';
import AdminPanel from './components/AdminPanel';

const API_BASE = process.env.REACT_APP_API_BASE || 'https://degenlogs.com:5001/api';
const TELEGRAM_BOT_USERNAME = process.env.REACT_APP_TELEGRAM_BOT_USERNAME || 'test_walletpulse_bot';

function App() {
  const [user, setUser] = useState(null);
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isCheckingAuth, setIsCheckingAuth] = useState(true);
  const [showAdminPanel, setShowAdminPanel] = useState(false);
  const [walletCount, setWalletCount] = useState(0);
  const [transactions, setTransactions] = useState([]);
  const [monitoringStatus, setMonitoringStatus] = useState({ isMonitoring: false });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const [timeframe, setTimeframe] = useState('24');
  const [transactionType, setTransactionType] = useState('all');
  const [groups, setGroups] = useState([]);
  const [selectedGroup, setSelectedGroup] = useState(null);
  const [selectedGroupInfo, setSelectedGroupInfo] = useState(null);

  const reconnectAttempts = useRef(0);
  const maxReconnectAttempts = 5;
  const reconnectDelay = useRef(1000); 
  const maxReconnectDelay = 30000; 
  const heartbeatTimeout = useRef(null);
  const HEARTBEAT_INTERVAL = 30000; 

  useEffect(() => {
    checkAuthentication();
  }, []);

  const checkAuthentication = async () => {
    const sessionToken = localStorage.getItem('sessionToken');
    const savedUser = localStorage.getItem('user');

    if (!sessionToken || !savedUser) {
      setIsCheckingAuth(false);
      return;
    }

    try {
      const response = await fetch(`${API_BASE}/auth/validate`, {
        headers: {
          'Authorization': `Bearer ${sessionToken}`,
        },
      });

      if (response.ok) {
        const userData = JSON.parse(savedUser);
        setUser(userData);
        setIsAuthenticated(true);
      } else {
        localStorage.removeItem('sessionToken');
        localStorage.removeItem('user');
      }
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Session check error:`, error);
      localStorage.removeItem('sessionToken');
      localStorage.removeItem('user');
    } finally {
      setIsCheckingAuth(false);
    }
  };

  const handleLogin = (authData) => {
    setUser(authData.user);
    setIsAuthenticated(true);
    setLoading(true);
    fastInit();
  };

  const handleLogout = () => {
    localStorage.removeItem('sessionToken');
    localStorage.removeItem('user');
    setUser(null);
    setIsAuthenticated(false);
    setShowAdminPanel(false);
    setTransactions([]);
    setError(null);
  };

  const getAuthHeaders = () => {
    const sessionToken = localStorage.getItem('sessionToken');
    return {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${sessionToken}`,
    };
  };

  const fastInit = async (hours = timeframe, type = transactionType, groupId = selectedGroup) => {
    try {
      setError(null);
      const startTime = Date.now();

      const headers = getAuthHeaders();
      const initUrl = `${API_BASE}/init?hours=${hours}${type !== 'all' ? `&type=${type}` : ''}${groupId ? `&groupId=${groupId}` : ''}`;

      const response = await fetch(initUrl, {
        headers,
        signal: AbortSignal.timeout(30000),
      });

      if (!response.ok) {
        throw new Error(`Failed to initialize application data: HTTP ${response.status}`);
      }

      const result = await response.json();

      if (!result.success || !result.data) {
        throw new Error('Invalid response format from server');
      }

      const { data } = result;

      setTransactions(data.transactions || []);
      setMonitoringStatus(data.monitoring || { isMonitoring: false });
      setGroups(data.groups || []);
      setWalletCount(data.wallets?.totalCount || 0);

      if (groupId && data.wallets?.selectedGroup) {
        const groupInfo = {
          groupId: data.wallets.selectedGroup.groupId,
          walletCount: data.wallets.selectedGroup.walletCount || 0,
          groupName: data.groups?.find((g) => g.id === groupId)?.name || 'Unknown Group',
        };
        setSelectedGroupInfo(groupInfo);
      } else {
        setSelectedGroupInfo(null);
      }

      const duration = Date.now() - startTime;
      console.log(`[${new Date().toISOString()}] ✅ Initialization completed in ${duration}ms`);
    } catch (err) {
      console.error(`[${new Date().toISOString()}] ❌ Error in init:`, err);
      setError(`Failed to load application data: ${err.message}`);
      setTransactions([]);
      setMonitoringStatus({ isMonitoring: false });
      setGroups([]);
      setWalletCount(0);
      setSelectedGroupInfo(null);
    } finally {
      setLoading(false);
    }
  };

  const removeAllWallets = async (groupId = null) => {
    try {
      const url = groupId ? `${API_BASE}/wallets?groupId=${groupId}` : `${API_BASE}/wallets`;
      const response = await fetch(url, {
        method: 'DELETE',
        headers: getAuthHeaders(),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.error || `HTTP ${response.status}: Failed to remove all wallets`);
      }

      const data = await response.json();

      if (data.newCounts) {
        setWalletCount(data.newCounts.totalWallets || 0);
        if (groupId && data.newCounts.selectedGroup) {
          setSelectedGroupInfo({
            groupId: data.newCounts.selectedGroup.groupId,
            walletCount: data.newCounts.selectedGroup.walletCount || 0,
            groupName: selectedGroupInfo?.groupName || 'Unknown Group',
          });
        } else if (groupId && !data.newCounts.selectedGroup) {
          setSelectedGroupInfo({
            groupId,
            walletCount: 0,
            groupName: selectedGroupInfo?.groupName || 'Unknown Group',
          });
        } else {
          setSelectedGroupInfo(null);
        }
      } else {
        if (groupId && selectedGroupInfo) {
          setSelectedGroupInfo({
            ...selectedGroupInfo,
            walletCount: 0,
          });
        } else {
          setWalletCount(0);
          setSelectedGroupInfo(null);
        }
      }

      setTransactions([]);
      setRefreshKey((prev) => prev + 1);

      return {
        success: true,
        message: data.message || `Successfully removed all wallets${groupId ? ' from selected group' : ''}`,
        newCounts: data.newCounts,
      };
    } catch (err) {
      console.error(`[${new Date().toISOString()}] ❌ Error in removeAllWallets:`, err);
      throw new Error(err.message || 'Failed to remove all wallets');
    }
  };

  const setupSSE = () => {
    if (!isAuthenticated) return;

    const sessionToken = localStorage.getItem('sessionToken');
    if (!sessionToken) return;

    const sseUrl = new URL(`${API_BASE}/transactions/stream`);
    sseUrl.searchParams.append('token', sessionToken);
    if (selectedGroup) {
      sseUrl.searchParams.append('groupId', selectedGroup);
    }

    const eventSource = new EventSource(sseUrl.toString());

    reconnectAttempts.current = 0;
    reconnectDelay.current = 1000;

    eventSource.addEventListener('ping', () => {
      console.log(`[${new Date().toISOString()}] 📡 SSE heartbeat received`);
      clearTimeout(heartbeatTimeout.current);
      heartbeatTimeout.current = setTimeout(() => {
        console.warn(`[${new Date().toISOString()}] ⚠️ Heartbeat timeout, attempting reconnect`);
        handleSSEError(eventSource, 'Heartbeat timeout');
      }, HEARTBEAT_INTERVAL * 2);
    });

    eventSource.onopen = () => {
      console.log(`[${new Date().toISOString()}] ✅ SSE connection established`);
      setError(null);
      reconnectAttempts.current = 0;
      reconnectDelay.current = 1000;
      clearTimeout(heartbeatTimeout.current);
      heartbeatTimeout.current = setTimeout(() => {
        console.warn(`[${new Date().toISOString()}] ⚠️ No initial heartbeat, attempting reconnect`);
        handleSSEError(eventSource, 'No initial heartbeat');
      }, HEARTBEAT_INTERVAL * 2);
    };

    eventSource.onmessage = (event) => {
      try {
        const newTransaction = JSON.parse(event.data);
        const now = new Date();
        const txTime = new Date(newTransaction.timestamp);
        const hoursDiff = (now - txTime) / (1000 * 60 * 60);
        const matchesTimeframe = hoursDiff <= parseInt(timeframe);
        const matchesType = transactionType === 'all' || newTransaction.transactionType === transactionType;
        const matchesGroup = !selectedGroup || newTransaction.groupId === selectedGroup;

        if (matchesTimeframe && matchesType && matchesGroup) {
          setTransactions((prev) => {
            if (prev.some((tx) => tx.signature === newTransaction.signature)) {
              return prev;
            }
            const formattedTransaction = {
              signature: newTransaction.signature,
              time: newTransaction.timestamp,
              transactionType: newTransaction.transactionType,
              solSpent: newTransaction.transactionType === 'buy' ? newTransaction.solAmount.toFixed(6) : null,
              solReceived: newTransaction.transactionType === 'sell' ? newTransaction.solAmount.toFixed(6) : null,
              wallet: {
                address: newTransaction.walletAddress,
                name: newTransaction.walletName || null,
                group_id: newTransaction.groupId,
                group_name: newTransaction.groupName,
              },
              tokensBought: newTransaction.transactionType === 'buy' ? newTransaction.tokens : [],
              tokensSold: newTransaction.transactionType === 'sell' ? newTransaction.tokens : [],
            };
            return [formattedTransaction, ...prev].slice(0, 4000);
          });
        }
      } catch (err) {
        console.error(`[${new Date().toISOString()}] ❌ Error parsing SSE message:`, err);
      }
    };

    eventSource.onerror = (error) => {
      console.error(`[${new Date().toISOString()}] ❌ SSE connection error:`, error);
      handleSSEError(eventSource, 'Connection error');
    };

    return () => {
      clearTimeout(heartbeatTimeout.current);
      eventSource.close();
      console.log(`[${new Date().toISOString()}] 🔌 SSE connection closed`);
    };
  };

  const handleSSEError = async (eventSource, errorReason) => {
    eventSource.close();
    clearTimeout(heartbeatTimeout.current);

    if (reconnectAttempts.current >= maxReconnectAttempts) {
      setError('Real-time connection lost. Please try reconnecting or refresh the page.');
      return;
    }

    try {
      const sessionToken = localStorage.getItem('sessionToken');
      const response = await fetch(`${API_BASE}/auth/validate`, {
        headers: {
          'Authorization': `Bearer ${sessionToken}`,
        },
      });

      if (!response.ok) {
        console.error(`[${new Date().toISOString()}] ❌ Session token invalid, logging out`);
        handleLogout();
        return;
      }
    } catch (err) {
      console.error(`[${new Date().toISOString()}] ❌ Session validation error:`, err);
      handleLogout();
      return;
    }

    reconnectAttempts.current += 1;
    console.warn(`[${new Date().toISOString()}] 🔄 Attempting SSE reconnect (${reconnectAttempts.current}/${maxReconnectAttempts}) in ${reconnectDelay.current}ms`);

    setTimeout(() => {
      setRefreshKey((prev) => prev + 1);
      reconnectDelay.current = Math.min(reconnectDelay.current * 2, maxReconnectDelay);
    }, reconnectDelay.current);
  };

  useEffect(() => {
    const cleanup = setupSSE();
    return cleanup;
  }, [timeframe, transactionType, selectedGroup, isAuthenticated, refreshKey]);

  const handleTimeframeChange = (newTimeframe) => {
    setTimeframe(newTimeframe);
    setLoading(true);
    setError(null);
    setTransactions([]);
    fastInit(newTimeframe, transactionType, selectedGroup);
  };

  const handleTransactionTypeChange = (newType) => {
    setTransactionType(newType);
    setLoading(true);
    setError(null);
    setTransactions([]);
    fastInit(timeframe, newType, selectedGroup);
  };

  const handleGroupChange = async (groupId) => {
    const selectedGroupId = groupId || null;
    setSelectedGroup(selectedGroupId);
    setLoading(true);
    setError(null);
    setTransactions([]);
    setSelectedGroupInfo(null);

    try {
      const response = await fetch(`${API_BASE}/groups/switch`, {
        method: 'POST',
        headers: getAuthHeaders(),
        body: JSON.stringify({ groupId: selectedGroupId }),
      });

      if (!response.ok) {
        throw new Error(`Failed to switch group: HTTP ${response.status}`);
      }

      fastInit(timeframe, transactionType, selectedGroupId);
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Error switching group:`, error);
      setError(`Failed to switch group: ${error.message}`);
      setLoading(false);
    }
  };

  const handleAddWalletsBulk = async (wallets, groupId, progressCallback) => {
    const startTime = Date.now();
    try {
      if (progressCallback) {
        progressCallback({
          current: 0,
          total: wallets.length,
          batch: 1,
          phase: 'validating',
        });
      }

      const CHUNK_SIZE = 1000;
      const chunks = [];
      for (let i = 0; i < wallets.length; i += CHUNK_SIZE) {
        chunks.push(wallets.slice(i, i + CHUNK_SIZE));
      }

      let totalResults = {
        total: wallets.length,
        successful: 0,
        failed: 0,
        errors: [],
        successfulWallets: [],
      };

      for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        if (progressCallback) {
          progressCallback({
            current: i * CHUNK_SIZE,
            total: wallets.length,
            batch: i + 1,
            phase: 'uploading',
          });
        }

        try {
          const response = await fetch(`${API_BASE}/wallets/bulk-optimized`, {
            method: 'POST',
            headers: getAuthHeaders(),
            body: JSON.stringify({
              wallets: chunk,
              groupId,
              optimized: true,
            }),
          });

          if (!response.ok) {
            throw new Error(`Chunk ${i + 1} failed: HTTP ${response.status}`);
          }

          const result = await response.json();
          if (!result.success && !result.results) {
            throw new Error(result.error || 'Unknown server error');
          }

          totalResults.successful += result.results.successful || 0;
          totalResults.failed += result.results.failed || 0;
          if (result.results.errors) {
            totalResults.errors.push(...result.results.errors);
          }
          if (result.results.successfulWallets) {
            totalResults.successfulWallets.push(...result.results.successfulWallets);
          }

          if (result.results.newCounts && result.results.successful > 0) {
            setWalletCount(result.results.newCounts.totalWallets);
            if (selectedGroupInfo && (!groupId || groupId === selectedGroupInfo.groupId)) {
              const newGroupCount = result.results.newCounts.groupCounts?.find(
                (gc) => gc.groupId === selectedGroupInfo.groupId
              )?.count;
              if (newGroupCount !== undefined) {
                setSelectedGroupInfo((prev) =>
                  prev
                    ? {
                        ...prev,
                        walletCount: newGroupCount,
                      }
                    : null
                );
              }
            }
          }
        } catch (chunkError) {
          console.error(`[${new Date().toISOString()}] ❌ Chunk ${i + 1} failed:`, chunkError.message);
          totalResults.failed += chunk.length;
          totalResults.errors.push({
            address: `chunk_${i + 1}`,
            error: `Entire chunk failed: ${chunkError.message}`,
            walletCount: chunk.length,
          });
        }

        if (i < chunks.length - 1) {
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
      }

      if (progressCallback) {
        progressCallback({
          current: wallets.length,
          total: wallets.length,
          batch: chunks.length,
          phase: 'completed',
        });
      }

      const duration = Date.now() - startTime;
      const walletsPerSecond = Math.round((totalResults.successful / duration) * 1000);
      const successRate = ((totalResults.successful / totalResults.total) * 100).toFixed(1);

      return {
        success: totalResults.successful > 0,
        message: `Import: ${totalResults.successful} successful, ${totalResults.failed} failed (${successRate}% success rate)`,
        results: totalResults,
      };
    } catch (error) {
      console.error(`[${new Date().toISOString()}] ❌ Bulk import failed:`, error);
      throw new Error(`Bulk import failed: ${error.message}`);
    }
  };

  const createGroup = async (name) => {
    try {
      const response = await fetch(`${API_BASE}/groups`, {
        method: 'POST',
        headers: getAuthHeaders(),
        body: JSON.stringify({ name }),
      });

      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Failed to create group');
      }

      setRefreshKey((prev) => prev + 1);
      return { success: true, message: data.message, group: data.group };
    } catch (err) {
      throw new Error(err.message);
    }
  };

  const toggleMonitoring = async (action) => {
    try {
      const response = await fetch(`${API_BASE}/monitoring/toggle`, {
        method: 'POST',
        headers: getAuthHeaders(),
        body: JSON.stringify({ action, groupId: selectedGroup }),
      });

      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Failed to toggle monitoring');
      }

      setRefreshKey((prev) => prev + 1);
    } catch (err) {
      setError(err.message);
    }
  };

  const handleReconnect = () => {
    setError(null);
    setRefreshKey((prev) => prev + 1);
  };

  useEffect(() => {
    if (isAuthenticated) {
      fastInit();
    }
  }, [isAuthenticated]);

  if (isCheckingAuth) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <LoadingSpinner />
      </div>
    );
  }

  if (!isAuthenticated) {
    return <TelegramLogin onLogin={handleLogin} botUsername={TELEGRAM_BOT_USERNAME} />;
  }

  if (loading) {
    return (
      <div className="h-screen bg-gray-900 flex flex-col">
        <Header user={user} onLogout={handleLogout} onOpenAdmin={() => setShowAdminPanel(true)} />
        <div className="flex-1 flex items-center justify-center">
          <div className="flex items-center space-x-3">
            <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-500"></div>
            <span className="text-white">Loading wallets...</span>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="h-screen bg-gray-900 flex flex-col overflow-hidden">
      <Header user={user} onLogout={handleLogout} onOpenAdmin={() => setShowAdminPanel(true)} />
      {error && (
        <ErrorMessage
          error={error}
          action={
            error.includes('Real-time connection lost') ? (
              <button
                onClick={handleReconnect}
                className="mt-2 px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
              >
                Reconnect
              </button>
            ) : null
          }
        />
      )}
      <MonitoringStatus status={monitoringStatus} onToggle={toggleMonitoring} />
      <WalletManager
        onAddWalletsBulk={handleAddWalletsBulk}
        onCreateGroup={createGroup}
        onRemoveAllWallets={removeAllWallets}
        groups={groups}
        selectedGroup={selectedGroup}
        selectedGroupInfo={selectedGroupInfo}
        walletCount={walletCount}
      />
      <div className="flex-1 overflow-hidden">
        <TokenTracker
          groupId={selectedGroup}
          transactions={transactions}
          timeframe={timeframe}
          onTimeframeChange={handleTimeframeChange}
          groups={groups}
          selectedGroup={selectedGroup}
          onGroupChange={handleGroupChange}
          walletCount={walletCount}
          selectedGroupInfo={selectedGroupInfo}
        />
      </div>
      {showAdminPanel && user?.isAdmin && (
        <AdminPanel user={user} onClose={() => setShowAdminPanel(false)} />
      )}
    </div>
  );
}

export default App;