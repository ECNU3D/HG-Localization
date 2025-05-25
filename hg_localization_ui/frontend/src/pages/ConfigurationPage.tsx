import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Save, AlertCircle, CheckCircle, Info } from 'lucide-react';
import { useConfigStatus, useSetConfig } from '../hooks/useConfig';
import { S3Config } from '../types';

export const ConfigurationPage: React.FC = () => {
  const navigate = useNavigate();
  const { data: configStatus, isLoading } = useConfigStatus();
  const setConfigMutation = useSetConfig();

  const [formData, setFormData] = useState<S3Config>({
    s3_bucket_name: '',
    s3_endpoint_url: '',
    aws_access_key_id: '',
    aws_secret_access_key: '',
    s3_data_prefix: '',
  });

  const [showCredentials, setShowCredentials] = useState(false);

  useEffect(() => {
    if (configStatus) {
      setFormData({
        s3_bucket_name: configStatus.bucket_name || '',
        s3_endpoint_url: configStatus.endpoint_url || '',
        aws_access_key_id: '',
        aws_secret_access_key: '',
        s3_data_prefix: configStatus.data_prefix || '',
      });
      setShowCredentials(configStatus.has_credentials);
    }
  }, [configStatus]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    const configToSubmit: S3Config = {
      s3_bucket_name: formData.s3_bucket_name,
      s3_endpoint_url: formData.s3_endpoint_url || undefined,
      s3_data_prefix: formData.s3_data_prefix || undefined,
    };

    if (showCredentials) {
      configToSubmit.aws_access_key_id = formData.aws_access_key_id || undefined;
      configToSubmit.aws_secret_access_key = formData.aws_secret_access_key || undefined;
    }

    try {
      await setConfigMutation.mutateAsync(configToSubmit);
      navigate('/datasets');
    } catch (error) {
      console.error('Configuration failed:', error);
    }
  };

  const handleInputChange = (field: keyof S3Config, value: string) => {
    setFormData(prev => ({
      ...prev,
      [field]: value,
    }));
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-96">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
      </div>
    );
  }

  return (
    <div className="max-w-2xl mx-auto">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900">Configuration</h1>
        <p className="mt-2 text-gray-600">
          Configure your S3 settings to access datasets. You can use public access (bucket name only) 
          or private access (with credentials).
        </p>
      </div>

      {/* Access Mode Selection */}
      <div className="card mb-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Access Mode</h2>
        <div className="space-y-4">
          <label className="flex items-start space-x-3 cursor-pointer">
            <input
              type="radio"
              name="accessMode"
              checked={!showCredentials}
              onChange={() => setShowCredentials(false)}
              className="mt-1 h-4 w-4 text-primary-600 focus:ring-primary-500 border-gray-300"
            />
            <div>
              <div className="font-medium text-gray-900">Public Access</div>
              <div className="text-sm text-gray-600">
                Access public datasets only. Requires only bucket name and optional endpoint.
              </div>
            </div>
          </label>
          
          <label className="flex items-start space-x-3 cursor-pointer">
            <input
              type="radio"
              name="accessMode"
              checked={showCredentials}
              onChange={() => setShowCredentials(true)}
              className="mt-1 h-4 w-4 text-primary-600 focus:ring-primary-500 border-gray-300"
            />
            <div>
              <div className="font-medium text-gray-900">Private Access</div>
              <div className="text-sm text-gray-600">
                Access both public and private datasets. Requires AWS credentials.
              </div>
            </div>
          </label>
        </div>
      </div>

      {/* Configuration Form */}
      <form onSubmit={handleSubmit} className="space-y-6">
        <div className="card">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">S3 Settings</h2>
          
          <div className="space-y-4">
            {/* S3 Bucket Name */}
            <div>
              <label className="label">
                S3 Bucket Name <span className="text-red-500">*</span>
              </label>
              <input
                type="text"
                required
                className="input"
                placeholder="my-datasets-bucket"
                value={formData.s3_bucket_name}
                onChange={(e) => handleInputChange('s3_bucket_name', e.target.value)}
              />
              <p className="mt-1 text-sm text-gray-500">
                The name of your S3 bucket where datasets are stored.
              </p>
            </div>

            {/* S3 Endpoint URL */}
            <div>
              <label className="label">S3 Endpoint URL</label>
              <input
                type="url"
                className="input"
                placeholder="https://s3.amazonaws.com (leave empty for AWS S3)"
                value={formData.s3_endpoint_url}
                onChange={(e) => handleInputChange('s3_endpoint_url', e.target.value)}
              />
              <p className="mt-1 text-sm text-gray-500">
                For S3-compatible services like MinIO. Leave empty for AWS S3.
              </p>
            </div>

            {/* S3 Data Prefix */}
            <div>
              <label className="label">S3 Data Prefix</label>
              <input
                type="text"
                className="input"
                placeholder="datasets/ (optional namespace)"
                value={formData.s3_data_prefix}
                onChange={(e) => handleInputChange('s3_data_prefix', e.target.value)}
              />
              <p className="mt-1 text-sm text-gray-500">
                Optional prefix to namespace your datasets within the bucket.
              </p>
            </div>
          </div>
        </div>

        {/* AWS Credentials (if private access) */}
        {showCredentials && (
          <div className="card">
            <h2 className="text-lg font-semibold text-gray-900 mb-4">AWS Credentials</h2>
            
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-4">
              <div className="flex">
                <Info className="w-5 h-5 text-blue-600 mt-0.5 mr-2 flex-shrink-0" />
                <div className="text-sm text-blue-800">
                  <p className="font-medium">Security Note</p>
                  <p>
                    Credentials are stored temporarily in memory and are not persisted. 
                    You'll need to re-enter them when you restart the application.
                  </p>
                </div>
              </div>
            </div>

            <div className="space-y-4">
              {/* AWS Access Key ID */}
              <div>
                <label className="label">
                  AWS Access Key ID <span className="text-red-500">*</span>
                </label>
                <input
                  type="text"
                  required={showCredentials}
                  className="input"
                  placeholder="AKIAIOSFODNN7EXAMPLE"
                  value={formData.aws_access_key_id}
                  onChange={(e) => handleInputChange('aws_access_key_id', e.target.value)}
                />
              </div>

              {/* AWS Secret Access Key */}
              <div>
                <label className="label">
                  AWS Secret Access Key <span className="text-red-500">*</span>
                </label>
                <input
                  type="password"
                  required={showCredentials}
                  className="input"
                  placeholder="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
                  value={formData.aws_secret_access_key}
                  onChange={(e) => handleInputChange('aws_secret_access_key', e.target.value)}
                />
              </div>
            </div>
          </div>
        )}

        {/* Current Status */}
        {configStatus?.configured && (
          <div className="card">
            <h2 className="text-lg font-semibold text-gray-900 mb-4">Current Status</h2>
            <div className="flex items-center space-x-2">
              <CheckCircle className="w-5 h-5 text-green-600" />
              <span className="text-green-800">
                Connected to bucket: <code className="bg-gray-100 px-2 py-1 rounded text-sm">
                  {configStatus.bucket_name}
                </code>
              </span>
            </div>
            <div className="mt-2 text-sm text-gray-600">
              Access level: {configStatus.has_credentials ? 'Private' : 'Public'}
            </div>
          </div>
        )}

        {/* Error Display */}
        {setConfigMutation.isError && (
          <div className="card bg-red-50 border-red-200">
            <div className="flex items-center space-x-2">
              <AlertCircle className="w-5 h-5 text-red-600" />
              <span className="text-red-800 font-medium">Configuration Error</span>
            </div>
            <p className="mt-2 text-sm text-red-700">
              {setConfigMutation.error?.message || 'Failed to save configuration. Please check your settings.'}
            </p>
          </div>
        )}

        {/* Submit Button */}
        <div className="flex justify-end space-x-4">
          <button
            type="submit"
            disabled={setConfigMutation.isPending || !formData.s3_bucket_name}
            className="btn-primary flex items-center space-x-2 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {setConfigMutation.isPending ? (
              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
            ) : (
              <Save className="w-4 h-4" />
            )}
            <span>
              {setConfigMutation.isPending ? 'Saving...' : 'Save Configuration'}
            </span>
          </button>
        </div>
      </form>
    </div>
  );
}; 