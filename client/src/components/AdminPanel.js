import React, { useState, useEffect } from 'react';

const AdminPanel = ({ user, onClose }) => {
  const [activeTab, setActiveTab] = useState('whitelist');
  const [whitelist, setWhitelist] = useState([]);
  const [users, setUsers] = useState([]);
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(false);
  const [newTelegramId, setNewTelegramId] = useState('');
  const [newUserNotes, setNewUserNotes] = useState('');
  const [error, setError] = useState('');

  const getApiBase = () => {
    if (window.location.hostname === 'localhost') {
      return 'http://localhost:5001/api';
    }
    return process.env.REACT_APP_API_BASE || '/api';
  };

  const getAuthHeaders = () => {
    const sessionToken = localStorage.getItem('sessionToken');
    return {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${sessionToken}`
    };
  };

  const fetchWithErrorHandling = async (url, options = {}) => {
    try {
      setError(''); 
      
      const response = await fetch(url, {
        ...options,
        headers: {
          ...getAuthHeaders(),
          ...options.headers
        }
      });

      if (!response.ok) {
        if (response.status === 401) {
          throw new Error('Session expired. Please log in again.');
        } else if (response.status === 403) {
          throw new Error('Access denied. Admin privileges required.');
        } else if (response.status >= 500) {
          throw new Error('Server error. Please try again later.');
        } else {
          const errorData = await response.json().catch(() => ({}));
          throw new Error(errorData.error || `Request failed with status ${response.status}`);
        }
      }

      return response;
    } catch (error) {
      if (error.name === 'TypeError' && error.message.includes('fetch')) {
        console.error('Network error details:', error);
        throw new Error('Network error: Unable to connect to the server. Please check your connection and try again.');
      }
      throw error;
    }
  };

  useEffect(() => {
    if (activeTab === 'whitelist') {
      fetchWhitelist();
    } else if (activeTab === 'users') {
      fetchUsers();
    } else if (activeTab === 'stats') {
      fetchStats();
    }
  }, [activeTab]);

  const fetchWhitelist = async () => {
    setLoading(true);
    try {
      const response = await fetchWithErrorHandling(`${getApiBase()}/admin/whitelist`);
      const data = await response.json();
      setWhitelist(data);
    } catch (error) {
      console.error('Error fetching whitelist:', error);
      setError(error.message);
    } finally {
      setLoading(false);
    }
  };

  const fetchUsers = async () => {
    setLoading(true);
    try {
      const response = await fetchWithErrorHandling(`${getApiBase()}/admin/users`);
      const data = await response.json();
      setUsers(data);
    } catch (error) {
      console.error('Error fetching users:', error);
      setError(error.message);
    } finally {
      setLoading(false);
    }
  };

  const fetchStats = async () => {
    setLoading(true);
    try {
      const response = await fetchWithErrorHandling(`${getApiBase()}/admin/stats`);
      const data = await response.json();
      setStats(data);
    } catch (error) {
      console.error('Error fetching stats:', error);
      setError(error.message);
    } finally {
      setLoading(false);
    }
  };

  const addToWhitelist = async () => {
    if (!newTelegramId.trim()) {
      setError('Telegram ID is required');
      return;
    }

    try {
      const response = await fetchWithErrorHandling(`${getApiBase()}/admin/whitelist`, {
        method: 'POST',
        body: JSON.stringify({
          telegramId: newTelegramId.trim(),
          notes: newUserNotes.trim()
        })
      });

      if (response.ok) {
        setNewTelegramId('');
        setNewUserNotes('');
        fetchWhitelist();
        setError(''); 
      }
    } catch (error) {
      console.error('Error adding to whitelist:', error);
      setError(error.message);
    }
  };

  const removeFromWhitelist = async (telegramId) => {
    if (!confirm(`Remove user ${telegramId} from whitelist?`)) return;

    try {
      const response = await fetchWithErrorHandling(`${getApiBase()}/admin/whitelist/${telegramId}`, {
        method: 'DELETE'
      });

      if (response.ok) {
        fetchWhitelist();
      }
    } catch (error) {
      console.error('Error removing from whitelist:', error);
      setError(error.message);
    }
  };

  const toggleUserStatus = async (userId, currentStatus) => {
    try {
      const response = await fetchWithErrorHandling(`${getApiBase()}/admin/users/${userId}/status`, {
        method: 'PATCH',
        body: JSON.stringify({
          isActive: !currentStatus
        })
      });

      if (response.ok) {
        fetchUsers();
      }
    } catch (error) {
      console.error('Error updating user status:', error);
      setError(error.message);
    }
  };

  const toggleAdminStatus = async (userId, currentStatus) => {
    if (!confirm('Change admin status for this user?')) return;

    try {
      const response = await fetchWithErrorHandling(`${getApiBase()}/admin/users/${userId}/admin`, {
        method: 'PATCH',
        body: JSON.stringify({
          isAdmin: !currentStatus
        })
      });

      if (response.ok) {
        fetchUsers();
      }
    } catch (error) {
      console.error('Error updating admin status:', error);
      setError(error.message);
    }
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-6xl max-h-[90vh] overflow-hidden">
        <div className="flex items-center justify-between p-6 border-b">
          <h2 className="text-2xl font-bold text-gray-900">Admin Panel</h2>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-600 p-2 rounded"
          >
            <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {error && (
          <div className="mx-6 mt-4 p-4 bg-red-50 border border-red-200 rounded-lg">
            <div className="flex items-center">
              <svg className="w-5 h-5 text-red-500 mr-2" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
              </svg>
              <span className="text-red-700 text-sm">{error}</span>
            </div>
          </div>
        )}

        <div className="flex border-b">
          {['whitelist', 'users', 'stats'].map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`px-6 py-3 font-medium text-sm capitalize ${
                activeTab === tab
                  ? 'text-blue-600 border-b-2 border-blue-600'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
            >
              {tab}
            </button>
          ))}
        </div>

        <div className="p-6 max-h-[60vh] overflow-y-auto">
          {activeTab === 'whitelist' && (
            <div className="space-y-6">
              <div className="bg-gray-50 p-4 rounded-lg">
                <h3 className="text-lg font-medium mb-4">Add User to Whitelist</h3>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <input
                    type="text"
                    placeholder="Telegram ID"
                    value={newTelegramId}
                    onChange={(e) => setNewTelegramId(e.target.value)}
                    className="px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                  />
                  <input
                    type="text"
                    placeholder="Notes (optional)"
                    value={newUserNotes}
                    onChange={(e) => setNewUserNotes(e.target.value)}
                    className="px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
                  />
                  <button
                    onClick={addToWhitelist}
                    disabled={loading}
                    className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed"
                  >
                    {loading ? 'Adding...' : 'Add to Whitelist'}
                  </button>
                </div>
              </div>

              {loading ? (
                <div className="text-center py-8">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto"></div>
                  <p className="mt-2 text-gray-600">Loading whitelist...</p>
                </div>
              ) : (
                <div className="space-y-2">
                  <h4 className="text-md font-medium text-gray-800 mb-3">Whitelisted Users</h4>
                  {whitelist.length === 0 ? (
                    <p className="text-gray-500">No users in whitelist</p>
                  ) : (
                    whitelist.map((entry, index) => (
                      <div key={index} className="flex items-center justify-between p-4 border rounded-lg">
                        <div className="flex-1">
                          <div className="font-medium">Telegram ID: {entry.telegram_id}</div>
                          {entry.notes && (
                            <div className="text-sm text-gray-600">Notes: {entry.notes}</div>
                          )}
                          <div className="text-xs text-gray-400">
                            Added: {new Date(entry.created_at).toLocaleDateString()}
                            {entry.added_by_username && ` • By: ${entry.added_by_username}`}
                          </div>
                        </div>
                        <button
                          onClick={() => removeFromWhitelist(entry.telegram_id)}
                          disabled={loading}
                          className="px-3 py-1 text-xs rounded bg-red-100 text-red-700 hover:bg-red-200 disabled:opacity-50"
                        >
                          Remove
                        </button>
                      </div>
                    ))
                  )}
                </div>
              )}
            </div>
          )}

          {activeTab === 'users' && (
            <div className="space-y-6">
              <h3 className="text-lg font-medium">Registered Users</h3>
              {loading ? (
                <div className="text-center py-8">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto"></div>
                  <p className="mt-2 text-gray-600">Loading users...</p>
                </div>
              ) : (
                <div className="space-y-2">
                  {users.length === 0 ? (
                    <p className="text-gray-500">No registered users</p>
                  ) : (
                    users.map((user) => (
                      <div key={user.id} className="flex items-center justify-between p-4 border rounded-lg">
                        <div className="flex-1">
                          <div className="flex items-center space-x-3">
                            <div>
                              <div className="font-medium">
                                {user.first_name} {user.last_name}
                                {user.username && <span className="text-gray-600"> (@{user.username})</span>}
                              </div>
                              <div className="text-sm text-gray-600">
                                Telegram ID: {user.telegram_id}
                              </div>
                              <div className="text-xs text-gray-400">
                                Joined: {new Date(user.created_at).toLocaleDateString()}
                                {user.last_login && ` • Last login: ${new Date(user.last_login).toLocaleDateString()}`}
                              </div>
                            </div>
                          </div>
                        </div>
                        <div className="flex items-center space-x-2">
                          <span className={`px-2 py-1 text-xs rounded-full ${
                            user.is_admin ? 'bg-purple-100 text-purple-800' : 'bg-gray-100 text-gray-600'
                          }`}>
                            {user.is_admin ? 'Admin' : 'User'}
                          </span>
                          <span className={`px-2 py-1 text-xs rounded-full ${
                            user.is_active ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
                          }`}>
                            {user.is_active ? 'Active' : 'Inactive'}
                          </span>
                          <button
                            onClick={() => toggleUserStatus(user.id, user.is_active)}
                            disabled={loading}
                            className={`px-3 py-1 text-xs rounded ${
                              user.is_active 
                                ? 'bg-red-100 text-red-700 hover:bg-red-200' 
                                : 'bg-green-100 text-green-700 hover:bg-green-200'
                            } disabled:opacity-50`}
                          >
                            {user.is_active ? 'Deactivate' : 'Activate'}
                          </button>
                          <button
                            onClick={() => toggleAdminStatus(user.id, user.is_admin)}
                            disabled={loading}
                            className={`px-3 py-1 text-xs rounded ${
                              user.is_admin
                                ? 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                                : 'bg-purple-100 text-purple-700 hover:bg-purple-200'
                            } disabled:opacity-50`}
                          >
                            {user.is_admin ? 'Remove Admin' : 'Make Admin'}
                          </button>
                        </div>
                      </div>
                    ))
                  )}
                </div>
              )}
            </div>
          )}

          {activeTab === 'stats' && (
            <div className="space-y-6">
              <h3 className="text-lg font-medium">System Statistics</h3>
              {loading ? (
                <div className="text-center py-8">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto"></div>
                  <p className="mt-2 text-gray-600">Loading statistics...</p>
                </div>
              ) : stats ? (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                  <div className="bg-blue-50 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-blue-600">{stats.total_users}</div>
                    <div className="text-sm text-gray-600">Total Users</div>
                  </div>
                  <div className="bg-green-50 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-green-600">{stats.active_users}</div>
                    <div className="text-sm text-gray-600">Active Users</div>
                  </div>
                  <div className="bg-purple-50 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-purple-600">{stats.total_wallets}</div>
                    <div className="text-sm text-gray-600">Total Wallets</div>
                  </div>
                  <div className="bg-yellow-50 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-yellow-600">{stats.total_transactions}</div>
                    <div className="text-sm text-gray-600">Total Transactions</div>
                  </div>
                  <div className="bg-indigo-50 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-indigo-600">{stats.total_groups}</div>
                    <div className="text-sm text-gray-600">Total Groups</div>
                  </div>
                  <div className="bg-pink-50 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-pink-600">{stats.whitelist_size}</div>
                    <div className="text-sm text-gray-600">Whitelisted Users</div>
                  </div>
                  <div className="bg-cyan-50 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-cyan-600">{Number(stats.total_sol_spent || 0).toFixed(2)}</div>
                    <div className="text-sm text-gray-600">Total SOL Spent</div>
                  </div>
                  <div className="bg-emerald-50 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-emerald-600">{Number(stats.total_sol_received || 0).toFixed(2)}</div>
                    <div className="text-sm text-gray-600">Total SOL Received</div>
                  </div>
                </div>
              ) : (
                <p className="text-gray-500">Unable to load statistics</p>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default AdminPanel;