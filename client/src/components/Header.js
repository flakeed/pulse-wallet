import React, { useState } from 'react';

function Header({ user, onLogout, onOpenAdmin }) {
  const [showUserMenu, setShowUserMenu] = useState(false);

  const handleLogoutClick = () => {
    setShowUserMenu(false);
    onLogout();
  };

  const handleAdminClick = () => {
    setShowUserMenu(false);
    onOpenAdmin();
  };

  return (
    <div className="bg-gray-900 border-b border-gray-700 px-4 py-2">
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <div className="text-white font-bold text-lg">WalletPulse</div>
          <div className="text-gray-400 text-xs hidden sm:block">Solana Monitor</div>
        </div>

        {user && (
          <div className="relative">
            <button
              onClick={() => setShowUserMenu(!showUserMenu)}
              className="flex items-center space-x-2 bg-gray-800 hover:bg-gray-700 rounded-md px-3 py-1.5 transition-colors"
            >
              <div className="w-6 h-6 bg-blue-600 rounded-full flex items-center justify-center">
                <span className="text-white text-xs font-medium">
                  {user.firstName?.[0]?.toUpperCase() || user.username?.[0]?.toUpperCase() || 'U'}
                </span>
              </div>
              <div className="text-left hidden sm:block">
                <div className="text-white text-sm font-medium">
                  {user.firstName} {user.lastName}
                </div>
                <div className="text-gray-400 text-xs">
                  {user.username ? `@${user.username}` : `ID: ${user.telegramId}`}
                  {user.isAdmin && <span className="ml-1 px-1 bg-purple-600 text-purple-100 rounded text-xs">Admin</span>}
                </div>
              </div>
              <svg className="w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
              </svg>
            </button>

            {showUserMenu && (
              <>
                <div className="absolute right-0 mt-2 w-64 bg-gray-800 rounded-lg shadow-lg border border-gray-700 z-50">
                  <div className="p-3 border-b border-gray-700">
                    <div className="flex items-center space-x-3">
                      <div className="w-8 h-8 bg-blue-600 rounded-full flex items-center justify-center">
                        <span className="text-white text-sm font-medium">
                          {user.firstName?.[0]?.toUpperCase() || user.username?.[0]?.toUpperCase() || 'U'}
                        </span>
                      </div>
                      <div>
                        <div className="text-white text-sm font-medium">
                          {user.firstName} {user.lastName}
                        </div>
                        <div className="text-gray-400 text-xs">
                          {user.username ? `@${user.username}` : `Telegram ID: ${user.telegramId}`}
                        </div>
                        {user.isAdmin && (
                          <div className="text-purple-400 text-xs font-medium">Administrator</div>
                        )}
                      </div>
                    </div>
                  </div>
                  
                  <div className="py-1">
                    {user.isAdmin && (
                      <button
                        onClick={handleAdminClick}
                        className="w-full px-4 py-2 text-left text-sm text-gray-300 hover:bg-gray-700 flex items-center space-x-2"
                      >
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                        </svg>
                        <span>Admin Panel</span>
                      </button>
                    )}
                    
                    <div className="border-t border-gray-700 my-1"></div>
                    
                    <button
                      onClick={handleLogoutClick}
                      className="w-full px-4 py-2 text-left text-sm text-red-400 hover:bg-gray-700 flex items-center space-x-2"
                    >
                      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" />
                      </svg>
                      <span>Sign Out</span>
                    </button>
                  </div>
                </div>

                <div 
                  className="fixed inset-0 z-40" 
                  onClick={() => setShowUserMenu(false)}
                ></div>
              </>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

export default Header;