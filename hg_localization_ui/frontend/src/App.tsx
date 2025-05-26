import React, { useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Layout } from './components/Layout';
import { ConfigurationPage } from './pages/ConfigurationPage';
import { DatasetsPage } from './pages/DatasetsPage';
import { DatasetDetailPage } from './pages/DatasetDetailPage';
import { useConfigStatus } from './hooks/useConfig';
import './index.css';

// Global error handler to suppress ResizeObserver errors
const suppressResizeObserverErrors = () => {
  const originalConsoleError = console.error;
  console.error = (...args) => {
    if (
      args[0]?.includes?.('ResizeObserver loop completed') ||
      args[0]?.includes?.('ResizeObserver loop limit exceeded')
    ) {
      return;
    }
    originalConsoleError.apply(console, args);
  };

  // Also handle window errors
  const originalWindowError = window.onerror;
  window.onerror = (message, source, lineno, colno, error) => {
    if (
      typeof message === 'string' && 
      (message.includes('ResizeObserver loop completed') ||
       message.includes('ResizeObserver loop limit exceeded'))
    ) {
      return true; // Prevent default error handling
    }
    if (originalWindowError) {
      return originalWindowError(message, source, lineno, colno, error);
    }
    return false;
  };
};

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 2,
      staleTime: 5 * 60 * 1000,
      refetchOnWindowFocus: false,
    },
  },
});

function AppContent() {
  const { data: configStatus, isLoading } = useConfigStatus();

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading configuration...</p>
        </div>
      </div>
    );
  }

  return (
    <Router>
      <Layout>
        <Routes>
          <Route 
            path="/" 
            element={
              configStatus?.configured ? 
                <Navigate to="/datasets" replace /> : 
                <Navigate to="/config" replace />
            } 
          />
          <Route path="/config" element={<ConfigurationPage />} />
          <Route 
            path="/datasets" 
            element={
              configStatus?.configured ? 
                <DatasetsPage /> : 
                <Navigate to="/config" replace />
            } 
          />
          <Route 
            path="/datasets/:datasetId" 
            element={
              configStatus?.configured ? 
                <DatasetDetailPage /> : 
                <Navigate to="/config" replace />
            } 
          />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </Layout>
    </Router>
  );
}

function App() {
  useEffect(() => {
    // Suppress ResizeObserver errors globally
    suppressResizeObserverErrors();
  }, []);

  return (
    <QueryClientProvider client={queryClient}>
      <AppContent />
    </QueryClientProvider>
  );
}

export default App; 