import axios, { AxiosResponse } from 'axios';
import {
  S3Config,
  DefaultConfig,
  ConfigStatus,
  DatasetInfo,
  DatasetDownloadRequest,
  DatasetPreview,
  CodeExample,
  DatasetCard,
  HealthStatus,
  ModelInfo,
  ModelDownloadRequest,
  ModelCard,
  ModelConfig,
  ModelTestingConfig,
  ModelTestRequest,
  ModelTestResponse,
  ModelAvailabilityCheck,
} from '../types';

const API_BASE_URL = process.env.REACT_APP_API_URL || '/api';

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  withCredentials: true, // Enable cookies for cross-origin requests
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor for logging
apiClient.interceptors.request.use(
  (config) => {
    console.log(`API Request: ${config.method?.toUpperCase()} ${config.url}`);
    return config;
  },
  (error) => {
    console.error('API Request Error:', error);
    return Promise.reject(error);
  }
);

// Response interceptor for error handling
apiClient.interceptors.response.use(
  (response) => {
    return response;
  },
  (error) => {
    console.error('API Response Error:', error.response?.data || error.message);
    
    // Check for authentication/authorization errors that might indicate expired credentials
    if (error.response?.status === 403 || error.response?.status === 401) {
      console.warn('Authentication error detected - credentials may be invalid');
      // Trigger a config status refresh by dispatching a custom event
      window.dispatchEvent(new CustomEvent('auth-error', { 
        detail: { 
          status: error.response?.status, 
          message: error.response?.data?.detail || 'Authentication failed' 
        } 
      }));
    }
    
    return Promise.reject(error);
  }
);

export const api = {
  // Configuration endpoints
  config: {
    setConfig: (config: S3Config): Promise<AxiosResponse<ConfigStatus>> =>
      apiClient.post('/config', config),
    
    getStatus: (): Promise<AxiosResponse<ConfigStatus>> =>
      apiClient.get('/config/status'),
    
    getDefaults: (): Promise<AxiosResponse<DefaultConfig>> =>
      apiClient.get('/config/defaults'),
    
    clearConfig: (): Promise<AxiosResponse<{ message: string }>> =>
      apiClient.delete('/config'),
  },

  // Dataset endpoints
  datasets: {
    getCached: (): Promise<AxiosResponse<DatasetInfo[]>> =>
      apiClient.get('/datasets/cached'),
    
    getS3: (): Promise<AxiosResponse<DatasetInfo[]>> =>
      apiClient.get('/datasets/s3'),
    
    getAll: (): Promise<AxiosResponse<DatasetInfo[]>> =>
      apiClient.get('/datasets/all'),
    
    cache: (request: DatasetDownloadRequest): Promise<AxiosResponse<{ message: string; dataset_id: string }>> =>
      apiClient.post('/datasets/cache', request),
    
    downloadZip: (
      datasetId: string,
      configName?: string,
      revision?: string
    ): Promise<AxiosResponse<Blob>> => {
      const params = new URLSearchParams();
      if (configName) params.append('config_name', configName);
      if (revision) params.append('revision', revision);
      
      return apiClient.get(`/datasets/${encodeURIComponent(datasetId)}/download?${params}`, {
        responseType: 'blob'
      });
    },
    
    getPreview: (
      datasetId: string,
      configName?: string,
      revision?: string,
      maxSamples: number = 5
    ): Promise<AxiosResponse<DatasetPreview>> => {
      const params = new URLSearchParams();
      if (configName) params.append('config_name', configName);
      if (revision) params.append('revision', revision);
      params.append('max_samples', maxSamples.toString());
      
      return apiClient.get(`/datasets/${encodeURIComponent(datasetId)}/preview?${params}`);
    },
    
    getCard: (
      datasetId: string,
      configName?: string,
      revision?: string
    ): Promise<AxiosResponse<DatasetCard>> => {
      const params = new URLSearchParams();
      if (configName) params.append('config_name', configName);
      if (revision) params.append('revision', revision);
      
      return apiClient.get(`/datasets/${encodeURIComponent(datasetId)}/card?${params}`);
    },
    
    getExamples: (
      datasetId: string,
      configName?: string,
      revision?: string
    ): Promise<AxiosResponse<CodeExample[]>> => {
      const params = new URLSearchParams();
      if (configName) params.append('config_name', configName);
      if (revision) params.append('revision', revision);
      
      return apiClient.get(`/datasets/${encodeURIComponent(datasetId)}/examples?${params}`);
    },
  },

  // Model endpoints
  models: {
    getCached: (): Promise<AxiosResponse<ModelInfo[]>> =>
      apiClient.get('/models/cached'),
    
    getS3: (): Promise<AxiosResponse<ModelInfo[]>> =>
      apiClient.get('/models/s3'),
    
    getAll: (): Promise<AxiosResponse<ModelInfo[]>> =>
      apiClient.get('/models/all'),
    
    cache: (request: ModelDownloadRequest): Promise<AxiosResponse<{ message: string; model_id: string }>> =>
      apiClient.post('/models/cache', request),
    
    getCard: (
      modelId: string,
      revision?: string
    ): Promise<AxiosResponse<ModelCard>> => {
      const params = new URLSearchParams();
      if (revision) params.append('revision', revision);
      
      return apiClient.get(`/models/${encodeURIComponent(modelId)}/card?${params}`);
    },
    
    getConfig: (
      modelId: string,
      revision?: string
    ): Promise<AxiosResponse<ModelConfig>> => {
      const params = new URLSearchParams();
      if (revision) params.append('revision', revision);
      
      return apiClient.get(`/models/${encodeURIComponent(modelId)}/config?${params}`);
    },
    
    getExamples: (
      modelId: string,
      revision?: string
    ): Promise<AxiosResponse<CodeExample[]>> => {
      const params = new URLSearchParams();
      if (revision) params.append('revision', revision);
      
      return apiClient.get(`/models/${encodeURIComponent(modelId)}/examples?${params}`);
    },
  },

  // Health check
  health: (): Promise<AxiosResponse<HealthStatus>> =>
    apiClient.get('/health'),

  // Model Testing endpoints (NEW)
  modelTesting: {
    getConfig: (): Promise<AxiosResponse<ModelTestingConfig>> =>
      apiClient.get('/model-testing/config'),
    
    checkAvailability: (
      modelId: string,
      apiKey: string
    ): Promise<AxiosResponse<ModelAvailabilityCheck>> => {
      const params = new URLSearchParams();
      params.append('model_id', modelId);
      params.append('api_key', apiKey);
      
      return apiClient.post(`/model-testing/check-availability?${params}`);
    },
    
    testModel: (request: ModelTestRequest): Promise<AxiosResponse<ModelTestResponse>> =>
      apiClient.post('/model-testing/test', request),
  },
};

// WebSocket connection for real-time updates
export class WebSocketClient {
  private ws: WebSocket | null = null;
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;
  private reconnectDelay = 1000;

  constructor(
    private url: string = `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/ws`,
    private onMessage?: (message: string) => void,
    private onError?: (error: Event) => void,
    private onClose?: () => void
  ) {}

  connect(): void {
    try {
      this.ws = new WebSocket(this.url);

      this.ws.onopen = () => {
        console.log('WebSocket connected');
        this.reconnectAttempts = 0;
      };

      this.ws.onmessage = (event) => {
        if (this.onMessage) {
          this.onMessage(event.data);
        }
      };

      this.ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        if (this.onError) {
          this.onError(error);
        }
      };

      this.ws.onclose = () => {
        console.log('WebSocket disconnected');
        if (this.onClose) {
          this.onClose();
        }
        this.attemptReconnect();
      };
    } catch (error) {
      console.error('Failed to create WebSocket connection:', error);
    }
  }

  private attemptReconnect(): void {
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++;
      console.log(`Attempting to reconnect... (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
      
      setTimeout(() => {
        this.connect();
      }, this.reconnectDelay * this.reconnectAttempts);
    } else {
      console.error('Max reconnection attempts reached');
    }
  }

  send(message: string): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(message);
    } else {
      console.warn('WebSocket is not connected');
    }
  }

  disconnect(): void {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  isConnected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }
}

export default api; 