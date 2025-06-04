import React, { useState, useMemo } from 'react';
import Link from 'next/link';
import { Search, Brain, Cloud, HardDrive, Filter, RefreshCw, Server, FileText, Settings, CheckCircle } from 'lucide-react';
import { useModels, useCacheModel } from '../hooks/useModels';
import { useConfigStatus } from '../hooks/useConfig';
import { ModelInfo } from '../types';

export const ModelsPage: React.FC = () => {
  const { data: models, isLoading, isFetching, refetch } = useModels();
  const { data: configStatus } = useConfigStatus();
  const cacheMutation = useCacheModel();

  const [searchTerm, setSearchTerm] = useState('');
  const [sourceFilter, setSourceFilter] = useState<'all' | 'cached' | 's3'>('all');
  const [cachingModels, setCachingModels] = useState<Set<string>>(new Set());

  const filteredModels = useMemo(() => {
    if (!models) return [];

    return models.filter(model => {
      const matchesSearch = model.model_id.toLowerCase().includes(searchTerm.toLowerCase()) ||
                           (model.revision?.toLowerCase().includes(searchTerm.toLowerCase()));
      
      let matchesSource = true;
      if (sourceFilter === 'cached') {
        matchesSource = model.is_cached;
      } else if (sourceFilter === 's3') {
        matchesSource = model.available_s3;
      }

      return matchesSearch && matchesSource;
    });
  }, [models, searchTerm, sourceFilter]);

  const handleCache = async (model: ModelInfo) => {
    const modelKey = `${model.model_id}_${model.revision}`;
    
    setCachingModels(prev => new Set(prev).add(modelKey));
    
    try {
      await cacheMutation.mutateAsync({
        model_id: model.model_id,
        revision: model.revision,
        make_public: false,
        metadata_only: true, // Default to metadata only
      });
    } catch (error) {
      console.error('Cache failed:', error);
    } finally {
      setCachingModels(prev => {
        const newSet = new Set(prev);
        newSet.delete(modelKey);
        return newSet;
      });
    }
  };

  const getModelKey = (model: ModelInfo) => 
    `${model.model_id}_${model.revision}`;

  const getModelIcon = (model: ModelInfo) => {
    if (model.is_full_model) {
      return <Brain className="w-5 h-5 text-green-600" />;
    }
    return <FileText className="w-5 h-5 text-blue-600" />;
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-96">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading models...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Models</h1>
          <p className="mt-2 text-gray-600">
            Browse and manage your Hugging Face models
          </p>
        </div>
        
        <div className="mt-4 sm:mt-0 flex items-center space-x-3">
          <div className="flex items-center space-x-2 text-sm text-gray-600">
            <div className={`w-2 h-2 rounded-full ${
              configStatus?.has_credentials && configStatus?.credentials_valid
                ? 'bg-green-500'
                : configStatus?.has_credentials && !configStatus?.credentials_valid
                ? 'bg-red-500'
                : 'bg-yellow-500'
            }`}></div>
            <span>
              {configStatus?.has_credentials && configStatus?.credentials_valid
                ? 'Private Access'
                : configStatus?.has_credentials && !configStatus?.credentials_valid
                ? 'Credentials Invalid (Public Access)'
                : 'Public Access'}
            </span>
          </div>
          
          <button
            onClick={() => refetch()}
            disabled={isFetching}
            className="btn-outline flex items-center space-x-2 disabled:opacity-50"
          >
            <RefreshCw className={`w-4 h-4 ${isFetching ? 'animate-spin' : ''}`} />
            <span>{isFetching ? 'Refreshing...' : 'Refresh'}</span>
          </button>
        </div>
      </div>

      <div className="card">
        <div className="flex flex-col sm:flex-row sm:items-center space-y-4 sm:space-y-0 sm:space-x-4">
          <div className="flex-1 relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
            <input
              type="text"
              placeholder="Search models..."
              className="input pl-10"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
            />
          </div>

          <div className="flex items-center space-x-2">
            <Filter className="w-4 h-4 text-gray-500" />
            <select
              value={sourceFilter}
              onChange={(e) => setSourceFilter(e.target.value as 'all' | 'cached' | 's3')}
              className="input w-auto"
            >
              <option value="all">All Sources</option>
              <option value="cached">Cached Only</option>
              <option value="s3">S3 Only</option>
            </select>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <div className="card">
          <div className="flex items-center">
            <Brain className="w-8 h-8 text-primary-600" />
            <div className="ml-3">
              <p className="text-sm font-medium text-gray-500">Total Models</p>
              <p className="text-2xl font-semibold text-gray-900">
                {filteredModels.length}
              </p>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="flex items-center">
            <HardDrive className="w-8 h-8 text-green-600" />
            <div className="ml-3">
              <p className="text-sm font-medium text-gray-500">Cached Models</p>
              <p className="text-2xl font-semibold text-gray-900">
                {filteredModels.filter(m => m.is_cached).length}
              </p>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="flex items-center">
            <Cloud className="w-8 h-8 text-blue-600" />
            <div className="ml-3">
              <p className="text-sm font-medium text-gray-500">S3 Models</p>
              <p className="text-2xl font-semibold text-gray-900">
                {filteredModels.filter(m => m.available_s3).length}
              </p>
            </div>
          </div>
        </div>
      </div>

      <div className="space-y-4 relative">
        {/* Loading overlay during refresh */}
        {isFetching && !isLoading && (
          <div className="absolute inset-0 bg-white bg-opacity-75 flex items-center justify-center z-50 rounded-lg">
            <div className="text-center">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600 mx-auto mb-2"></div>
              <p className="text-sm text-gray-600">Refreshing models...</p>
            </div>
          </div>
        )}
        
        {filteredModels.length === 0 ? (
          <div className="card text-center py-12">
            <Brain className="w-12 h-12 text-gray-400 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-gray-900 mb-2">No models found</h3>
            <p className="text-gray-600 mb-4">
              {searchTerm || sourceFilter !== 'all' 
                ? 'Try adjusting your search or filters.'
                : 'No models are available. Configure your S3 settings or upload some models.'}
            </p>
            {!searchTerm && sourceFilter === 'all' && (
              <Link href="/config" className="btn-primary">
                Configure S3 Settings
              </Link>
            )}
          </div>
        ) : (
          filteredModels.map((model) => {
            const modelKey = getModelKey(model);
            const isCaching = cachingModels.has(modelKey);
            
            return (
              <div key={modelKey} className="card hover:shadow-md transition-shadow relative">
                {/* Individual loading mask during cache operation */}
                {isCaching && (
                  <div className="absolute inset-0 bg-white bg-opacity-75 flex items-center justify-center z-40 rounded-lg">
                    <div className="text-center">
                      <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-primary-600 mx-auto mb-1"></div>
                      <p className="text-xs text-gray-600">Caching...</p>
                    </div>
                  </div>
                )}
                
                <div className="flex items-center justify-between">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center space-x-3">
                      <Link
                        href={`/models/${encodeURIComponent(model.model_id)}${model.revision ? `?revision=${encodeURIComponent(model.revision)}` : ''}`}
                        className="text-lg font-semibold text-primary-600 hover:text-primary-700 truncate flex items-center space-x-2"
                      >
                        {getModelIcon(model)}
                        <span>{model.model_id}</span>
                      </Link>
                      
                      <div className="flex items-center space-x-2">
                        {model.is_cached && (
                          <span className="badge badge-success">
                            <HardDrive className="w-3 h-3 mr-1" />
                            Cached
                          </span>
                        )}
                        
                        {model.available_s3 && (
                          <span className="badge badge-info">
                            <Cloud className="w-3 h-3 mr-1" />
                            S3
                          </span>
                        )}
                        
                        {model.is_full_model && (
                          <span className="badge badge-success">
                            <CheckCircle className="w-3 h-3 mr-1" />
                            Full Model
                          </span>
                        )}
                        
                        {model.has_card && (
                          <span className="badge badge-secondary">
                            📄 Card
                          </span>
                        )}
                        
                        {model.has_config && (
                          <span className="badge badge-secondary">
                            <Settings className="w-3 h-3 mr-1" />
                            Config
                          </span>
                        )}
                      </div>
                    </div>

                    <div className="mt-2 flex items-center space-x-4 text-sm text-gray-600">
                      {model.revision && (
                        <span>
                          <strong>Revision:</strong> {model.revision}
                        </span>
                      )}
                      {model.path && (
                        <span>
                          <strong>Path:</strong> {model.path}
                        </span>
                      )}
                      <span>
                        <strong>Type:</strong> {model.is_full_model ? 'Full Model' : 'Metadata Only'}
                      </span>
                      {model.has_tokenizer && (
                        <span>
                          <strong>Tokenizer:</strong> Yes
                        </span>
                      )}
                    </div>
                  </div>

                  <div className="flex items-center space-x-2 ml-4">
                    {/* Cache button - only show for S3 models that aren't cached yet */}
                    {model.available_s3 && !model.is_cached && (
                      <button
                        onClick={() => handleCache(model)}
                        disabled={isCaching}
                        className="btn-outline flex items-center space-x-2 disabled:opacity-50"
                        title="Cache model on server"
                      >
                        {isCaching ? (
                          <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-primary-600"></div>
                        ) : (
                          <Server className="w-4 h-4" />
                        )}
                        <span>
                          {isCaching ? 'Caching...' : 'Cache'}
                        </span>
                      </button>
                    )}

                    <Link
                      href={`/models/${encodeURIComponent(model.model_id)}${model.revision ? `?revision=${encodeURIComponent(model.revision)}` : ''}`}
                      className="btn-primary"
                    >
                      View Details
                    </Link>
                  </div>
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}; 