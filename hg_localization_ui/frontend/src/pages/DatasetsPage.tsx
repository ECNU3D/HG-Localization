import React, { useState, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { Search, Download, Database, Cloud, HardDrive, Filter, RefreshCw, Server } from 'lucide-react';
import { useDatasets, useCacheDataset, useDownloadDatasetZip } from '../hooks/useDatasets';
import { useConfigStatus } from '../hooks/useConfig';
import { DatasetInfo } from '../types';

export const DatasetsPage: React.FC = () => {
  const { data: datasets, isLoading, refetch } = useDatasets();
  const { data: configStatus } = useConfigStatus();
  const cacheMutation = useCacheDataset();
  const downloadZipMutation = useDownloadDatasetZip();

  const [searchTerm, setSearchTerm] = useState('');
  const [sourceFilter, setSourceFilter] = useState<'all' | 'cached' | 's3'>('all');
  const [downloadingDatasets, setDownloadingDatasets] = useState<Set<string>>(new Set());

  const filteredDatasets = useMemo(() => {
    if (!datasets) return [];

    return datasets.filter(dataset => {
      const matchesSearch = dataset.dataset_id.toLowerCase().includes(searchTerm.toLowerCase()) ||
                           (dataset.config_name?.toLowerCase().includes(searchTerm.toLowerCase())) ||
                           (dataset.revision?.toLowerCase().includes(searchTerm.toLowerCase()));
      
      let matchesSource = true;
      if (sourceFilter === 'cached') {
        matchesSource = dataset.is_cached;
      } else if (sourceFilter === 's3') {
        matchesSource = dataset.available_s3;
      }

      return matchesSearch && matchesSource;
    });
  }, [datasets, searchTerm, sourceFilter]);

  const handleCache = async (dataset: DatasetInfo) => {
    const datasetKey = `${dataset.dataset_id}_${dataset.config_name}_${dataset.revision}`;
    
    setDownloadingDatasets(prev => new Set(prev).add(datasetKey));
    
    try {
      await cacheMutation.mutateAsync({
        dataset_id: dataset.dataset_id,
        config_name: dataset.config_name,
        revision: dataset.revision,
        trust_remote_code: false,
        make_public: false,
      });
    } catch (error) {
      console.error('Cache failed:', error);
    } finally {
      setDownloadingDatasets(prev => {
        const newSet = new Set(prev);
        newSet.delete(datasetKey);
        return newSet;
      });
    }
  };

  const handleDownload = async (dataset: DatasetInfo) => {
    try {
      await downloadZipMutation.mutateAsync({
        datasetId: dataset.dataset_id,
        configName: dataset.config_name,
        revision: dataset.revision,
      });
    } catch (error) {
      console.error('Download failed:', error);
    }
  };

  const getDatasetKey = (dataset: DatasetInfo) => 
    `${dataset.dataset_id}_${dataset.config_name}_${dataset.revision}`;

  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-96">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading datasets...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Datasets</h1>
          <p className="mt-2 text-gray-600">
            Browse and manage your Hugging Face datasets
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
            className="btn-outline flex items-center space-x-2"
          >
            <RefreshCw className="w-4 h-4" />
            <span>Refresh</span>
          </button>
        </div>
      </div>

      <div className="card">
        <div className="flex flex-col sm:flex-row sm:items-center space-y-4 sm:space-y-0 sm:space-x-4">
          <div className="flex-1 relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
            <input
              type="text"
              placeholder="Search datasets..."
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
            <Database className="w-8 h-8 text-primary-600" />
            <div className="ml-3">
              <p className="text-sm font-medium text-gray-500">Total Datasets</p>
              <p className="text-2xl font-semibold text-gray-900">
                {filteredDatasets.length}
              </p>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="flex items-center">
            <HardDrive className="w-8 h-8 text-green-600" />
            <div className="ml-3">
              <p className="text-sm font-medium text-gray-500">Cached Datasets</p>
              <p className="text-2xl font-semibold text-gray-900">
                {filteredDatasets.filter(d => d.is_cached).length}
              </p>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="flex items-center">
            <Cloud className="w-8 h-8 text-blue-600" />
            <div className="ml-3">
              <p className="text-sm font-medium text-gray-500">S3 Datasets</p>
              <p className="text-2xl font-semibold text-gray-900">
                {filteredDatasets.filter(d => d.available_s3).length}
              </p>
            </div>
          </div>
        </div>
      </div>

      <div className="space-y-4">
        {filteredDatasets.length === 0 ? (
          <div className="card text-center py-12">
            <Database className="w-12 h-12 text-gray-400 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-gray-900 mb-2">No datasets found</h3>
            <p className="text-gray-600 mb-4">
              {searchTerm || sourceFilter !== 'all' 
                ? 'Try adjusting your search or filters.'
                : 'No datasets are available. Configure your S3 settings or download some datasets.'}
            </p>
            {!searchTerm && sourceFilter === 'all' && (
              <Link to="/config" className="btn-primary">
                Configure S3 Settings
              </Link>
            )}
          </div>
        ) : (
          filteredDatasets.map((dataset) => {
            const datasetKey = getDatasetKey(dataset);
            const isDownloading = downloadingDatasets.has(datasetKey);
            
            return (
              <div key={datasetKey} className="card hover:shadow-md transition-shadow">
                <div className="flex items-center justify-between">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center space-x-3">
                      <Link
                        to={`/datasets/${encodeURIComponent(dataset.dataset_id)}?config=${dataset.config_name || ''}&revision=${dataset.revision || ''}`}
                        className="text-lg font-semibold text-primary-600 hover:text-primary-700 truncate"
                      >
                        {dataset.dataset_id}
                      </Link>
                      
                      <div className="flex items-center space-x-2">
                        {dataset.is_cached && (
                          <span className="badge badge-success">
                            <HardDrive className="w-3 h-3 mr-1" />
                            Cached
                          </span>
                        )}
                        
                        {dataset.available_s3 && (
                          <span className="badge badge-info">
                            <Cloud className="w-3 h-3 mr-1" />
                            S3
                          </span>
                        )}
                        
                        {dataset.has_card && (
                          <span className="badge badge-secondary">
                            📄 Card
                          </span>
                        )}
                      </div>
                    </div>

                    <div className="mt-2 flex items-center space-x-4 text-sm text-gray-600">
                      {dataset.config_name && (
                        <span>
                          <strong>Config:</strong> {dataset.config_name}
                        </span>
                      )}
                      {dataset.revision && (
                        <span>
                          <strong>Revision:</strong> {dataset.revision}
                        </span>
                      )}
                      {dataset.path && (
                        <span>
                          <strong>Path:</strong> {dataset.path}
                        </span>
                      )}
                    </div>
                  </div>

                  <div className="flex items-center space-x-2 ml-4">
                    {/* Cache button - only show for S3 datasets that aren't cached yet */}
                    {dataset.available_s3 && !dataset.is_cached && (
                      <button
                        onClick={() => handleCache(dataset)}
                        disabled={isDownloading}
                        className="btn-outline flex items-center space-x-2 disabled:opacity-50"
                        title="Cache dataset on server"
                      >
                        {isDownloading ? (
                          <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-primary-600"></div>
                        ) : (
                          <Server className="w-4 h-4" />
                        )}
                        <span>
                          {isDownloading ? 'Caching...' : 'Cache'}
                        </span>
                      </button>
                    )}

                    {/* Download button - only show for cached datasets */}
                    {dataset.is_cached && (
                      <button
                        onClick={() => handleDownload(dataset)}
                        disabled={downloadZipMutation.isPending}
                        className="btn-primary flex items-center space-x-2 disabled:opacity-50"
                        title="Download dataset ZIP to your computer"
                      >
                        {downloadZipMutation.isPending ? (
                          <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
                        ) : (
                          <Download className="w-4 h-4" />
                        )}
                        <span>
                          {downloadZipMutation.isPending ? 'Downloading...' : 'Download ZIP'}
                        </span>
                      </button>
                    )}

                    <Link
                      to={`/datasets/${encodeURIComponent(dataset.dataset_id)}?config=${dataset.config_name || ''}&revision=${dataset.revision || ''}`}
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