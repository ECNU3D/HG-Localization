import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { 
  Brain, 
  Download, 
  Search, 
  Plus, 
  FileText, 
  Settings, 
  CheckCircle, 
  AlertCircle,
  Loader2,
  RefreshCw
} from 'lucide-react';
import { useCachedModels, useCacheModel, getModelTypeDisplay } from '../hooks/useModels';
import { ModelInfo, ModelDownloadRequest } from '../types';
import { WebSocketClient } from '../api/client';

export const ModelsPage: React.FC = () => {
  const [searchTerm, setSearchTerm] = useState('');
  const [showCacheDialog, setShowCacheDialog] = useState(false);
  const [cacheForm, setCacheForm] = useState<ModelDownloadRequest>({
    model_id: '',
    revision: '',
    make_public: false,
    metadata_only: true,
  });
  const [wsMessages, setWsMessages] = useState<string[]>([]);

  const { data: cachedModels = [], isLoading, error, refetch } = useCachedModels();
  const cacheModelMutation = useCacheModel();

  // WebSocket setup for real-time updates
  useEffect(() => {
    const client = new WebSocketClient(
      `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.hostname}:8000/ws`,
      (message) => {
        // Only show model-related messages, filter out dev server messages
        if (message.includes('model') || message.includes('Model') || message.includes('caching') || message.includes('cached')) {
          setWsMessages(prev => [...prev.slice(-4), message]); // Keep last 5 messages
        }
        // Auto-refresh models list when caching completes
        if (message.includes('Successfully cached') || message.includes('Failed to cache')) {
          setTimeout(() => refetch(), 1000);
        }
      },
      (error) => console.error('WebSocket error:', error),
      () => console.log('WebSocket disconnected')
    );
    
    client.connect();

    return () => {
      client.disconnect();
    };
  }, [refetch]);

  // Filter models based on search term
  const filteredModels = cachedModels.filter(model =>
    model.model_id.toLowerCase().includes(searchTerm.toLowerCase()) ||
    (model.revision && model.revision.toLowerCase().includes(searchTerm.toLowerCase()))
  );

  const handleCacheModel = async () => {
    if (!cacheForm.model_id.trim()) return;

    try {
      await cacheModelMutation.mutateAsync(cacheForm);
      setShowCacheDialog(false);
      setCacheForm({
        model_id: '',
        revision: '',
        make_public: false,
        metadata_only: true,
      });
    } catch (error) {
      console.error('Failed to cache model:', error);
    }
  };

  const getModelIcon = (model: ModelInfo) => {
    if (model.is_full_model) {
      return <Brain className="w-5 h-5 text-green-600" />;
    }
    return <FileText className="w-5 h-5 text-blue-600" />;
  };

  const getModelBadgeColor = (model: ModelInfo) => {
    return model.is_full_model ? 'bg-green-100 text-green-800' : 'bg-blue-100 text-blue-800';
  };

  if (error) {
    return (
      <div className="text-center py-12">
        <AlertCircle className="w-12 h-12 text-red-500 mx-auto mb-4" />
        <h3 className="text-lg font-medium text-gray-900 mb-2">Error Loading Models</h3>
        <p className="text-gray-600 mb-4">Failed to load cached models. Please try again.</p>
        <button
          onClick={() => refetch()}
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700"
        >
          <RefreshCw className="w-4 h-4 mr-2" />
          Retry
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Models</h1>
          <p className="text-gray-600">Manage your cached model metadata and full models</p>
        </div>
        <button
          onClick={() => setShowCacheDialog(true)}
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
        >
          <Plus className="w-4 h-4 mr-2" />
          Cache Model
        </button>
      </div>

      {/* Real-time messages */}
      {wsMessages.length > 0 && (
        <div className="bg-blue-50 border border-blue-200 rounded-md p-4">
          <h4 className="text-sm font-medium text-blue-800 mb-2">Model Caching Updates</h4>
          <div className="space-y-1">
            {wsMessages.map((message, index) => (
              <p key={index} className="text-sm text-blue-700">{message}</p>
            ))}
          </div>
        </div>
      )}

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
        <input
          type="text"
          placeholder="Search models..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-md focus:ring-primary-500 focus:border-primary-500"
        />
      </div>

      {/* Models List */}
      {isLoading ? (
        <div className="text-center py-12">
          <Loader2 className="w-8 h-8 text-primary-600 animate-spin mx-auto mb-4" />
          <p className="text-gray-600">Loading models...</p>
        </div>
      ) : filteredModels.length === 0 ? (
        <div className="text-center py-12">
          <Brain className="w-12 h-12 text-gray-400 mx-auto mb-4" />
          <h3 className="text-lg font-medium text-gray-900 mb-2">
            {searchTerm ? 'No models found' : 'No models cached yet'}
          </h3>
          <p className="text-gray-600 mb-4">
            {searchTerm 
              ? 'Try adjusting your search terms.' 
              : 'Start by caching a model from Hugging Face Hub.'
            }
          </p>
          {!searchTerm && (
            <button
              onClick={() => setShowCacheDialog(true)}
              className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700"
            >
              <Plus className="w-4 h-4 mr-2" />
              Cache Your First Model
            </button>
          )}
        </div>
      ) : (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {filteredModels.map((model) => (
            <Link
              key={`${model.model_id}-${model.revision || 'default'}`}
              to={`/models/${encodeURIComponent(model.model_id)}${model.revision ? `?revision=${encodeURIComponent(model.revision)}` : ''}`}
              className="block bg-white rounded-lg border border-gray-200 p-6 hover:shadow-md transition-shadow"
            >
              <div className="flex items-start justify-between mb-3">
                <div className="flex items-center space-x-2 flex-1 min-w-0">
                  {getModelIcon(model)}
                  <h3 className="text-lg font-medium text-gray-900 break-words">
                    {model.model_id}
                  </h3>
                </div>
                <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium flex-shrink-0 ml-2 ${getModelBadgeColor(model)}`}>
                  {getModelTypeDisplay(model)}
                </span>
              </div>

              {model.revision && model.revision !== 'default' && (
                <p className="text-sm text-gray-600 mb-2">
                  Revision: {model.revision}
                </p>
              )}

              <div className="flex items-center space-x-4 text-sm text-gray-500">
                <div className="flex items-center space-x-1">
                  <FileText className="w-4 h-4" />
                  <span>Card: {model.has_card ? 'Yes' : 'No'}</span>
                </div>
                <div className="flex items-center space-x-1">
                  <Settings className="w-4 h-4" />
                  <span>Config: {model.has_config ? 'Yes' : 'No'}</span>
                </div>
              </div>

              {model.is_full_model && (
                <div className="mt-2 flex items-center space-x-1 text-sm text-green-600">
                  <CheckCircle className="w-4 h-4" />
                  <span>Full model with weights</span>
                </div>
              )}
            </Link>
          ))}
        </div>
      )}

      {/* Cache Model Dialog */}
      {showCacheDialog && (
        <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full z-50">
          <div className="relative top-20 mx-auto p-5 border w-96 shadow-lg rounded-md bg-white">
            <div className="mt-3">
              <h3 className="text-lg font-medium text-gray-900 mb-4">Cache Model</h3>
              
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Model ID *
                  </label>
                  <input
                    type="text"
                    placeholder="e.g., bert-base-uncased, microsoft/DialoGPT-medium"
                    value={cacheForm.model_id}
                    onChange={(e) => setCacheForm(prev => ({ ...prev, model_id: e.target.value }))}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-primary-500 focus:border-primary-500"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Revision (optional)
                  </label>
                  <input
                    type="text"
                    placeholder="main, v1.0, commit hash..."
                    value={cacheForm.revision}
                    onChange={(e) => setCacheForm(prev => ({ ...prev, revision: e.target.value }))}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-primary-500 focus:border-primary-500"
                  />
                </div>

                <div className="space-y-2">
                  <label className="flex items-center">
                    <input
                      type="radio"
                      name="downloadType"
                      checked={cacheForm.metadata_only}
                      onChange={() => setCacheForm(prev => ({ ...prev, metadata_only: true }))}
                      className="mr-2"
                    />
                    <span className="text-sm text-gray-700">Metadata only (fast, recommended)</span>
                  </label>
                  <label className="flex items-center">
                    <input
                      type="radio"
                      name="downloadType"
                      checked={!cacheForm.metadata_only}
                      onChange={() => setCacheForm(prev => ({ ...prev, metadata_only: false }))}
                      className="mr-2"
                    />
                    <span className="text-sm text-gray-700">Full model (includes weights, large download)</span>
                  </label>
                </div>

                <div>
                  <label className="flex items-center">
                    <input
                      type="checkbox"
                      checked={cacheForm.make_public}
                      onChange={(e) => setCacheForm(prev => ({ ...prev, make_public: e.target.checked }))}
                      className="mr-2"
                    />
                    <span className="text-sm text-gray-700">Make public (requires S3)</span>
                  </label>
                </div>
              </div>

              <div className="flex justify-end space-x-3 mt-6">
                <button
                  onClick={() => setShowCacheDialog(false)}
                  className="px-4 py-2 text-sm font-medium text-gray-700 bg-gray-100 hover:bg-gray-200 rounded-md"
                >
                  Cancel
                </button>
                <button
                  onClick={handleCacheModel}
                  disabled={!cacheForm.model_id.trim() || cacheModelMutation.isPending}
                  className="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-md disabled:opacity-50 disabled:cursor-not-allowed flex items-center"
                >
                  {cacheModelMutation.isPending ? (
                    <>
                      <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                      Caching...
                    </>
                  ) : (
                    <>
                      <Download className="w-4 h-4 mr-2" />
                      Cache Model
                    </>
                  )}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}; 