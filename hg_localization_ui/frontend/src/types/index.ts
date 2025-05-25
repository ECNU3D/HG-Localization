export interface S3Config {
  s3_bucket_name: string;
  s3_endpoint_url?: string;
  aws_access_key_id?: string;
  aws_secret_access_key?: string;
  s3_data_prefix?: string;
}

export interface ConfigStatus {
  configured: boolean;
  has_credentials: boolean;
  bucket_name?: string;
  endpoint_url?: string;
  data_prefix?: string;
}

export interface DatasetInfo {
  dataset_id: string;
  config_name?: string;
  revision?: string;
  path?: string;
  has_card: boolean;
  s3_card_url?: string;
  source: 'cached' | 's3' | 'both';
  is_cached: boolean;
  available_s3: boolean;
}

export interface DatasetDownloadRequest {
  dataset_id: string;
  config_name?: string;
  revision?: string;
  trust_remote_code: boolean;
  make_public: boolean;
}

export interface DatasetPreview {
  dataset_id: string;
  config_name?: string;
  revision?: string;
  features: Record<string, any>;
  num_rows: Record<string, number>;
  sample_data: Record<string, any>[];
}

export interface CodeExample {
  title: string;
  description: string;
  code: string;
  language: string;
}

export interface DatasetCard {
  content: string;
}

export interface DownloadProgress {
  dataset_id: string;
  status: 'starting' | 'downloading' | 'completed' | 'failed';
  message: string;
  progress?: number;
}

export interface ApiResponse<T> {
  data?: T;
  error?: string;
  message?: string;
}

export interface HealthStatus {
  status: string;
  service: string;
} 