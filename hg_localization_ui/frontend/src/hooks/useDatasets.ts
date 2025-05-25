import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { api } from '../api/client';
import { DatasetDownloadRequest } from '../types';

export const useDatasets = () => {
  return useQuery({
    queryKey: ['datasets', 'all'],
    queryFn: async () => {
      const response = await api.datasets.getAll();
      return response.data;
    },
    staleTime: 1000 * 60 * 2, // 2 minutes
  });
};

export const useCachedDatasets = () => {
  return useQuery({
    queryKey: ['datasets', 'cached'],
    queryFn: async () => {
      const response = await api.datasets.getCached();
      return response.data;
    },
    staleTime: 1000 * 60 * 2, // 2 minutes
  });
};

export const useS3Datasets = () => {
  return useQuery({
    queryKey: ['datasets', 's3'],
    queryFn: async () => {
      const response = await api.datasets.getS3();
      return response.data;
    },
    staleTime: 1000 * 60 * 2, // 2 minutes
  });
};

export const useDatasetPreview = (
  datasetId: string,
  configName?: string,
  revision?: string,
  enabled: boolean = true
) => {
  return useQuery({
    queryKey: ['dataset', 'preview', datasetId, configName, revision],
    queryFn: async () => {
      const response = await api.datasets.getPreview(datasetId, configName, revision);
      return response.data;
    },
    enabled: enabled && !!datasetId,
    staleTime: 1000 * 60 * 10, // 10 minutes
  });
};

export const useDatasetCard = (
  datasetId: string,
  configName?: string,
  revision?: string,
  enabled: boolean = true
) => {
  return useQuery({
    queryKey: ['dataset', 'card', datasetId, configName, revision],
    queryFn: async () => {
      const response = await api.datasets.getCard(datasetId, configName, revision);
      return response.data;
    },
    enabled: enabled && !!datasetId,
    staleTime: 1000 * 60 * 30, // 30 minutes
  });
};

export const useDatasetExamples = (
  datasetId: string,
  configName?: string,
  revision?: string,
  enabled: boolean = true
) => {
  return useQuery({
    queryKey: ['dataset', 'examples', datasetId, configName, revision],
    queryFn: async () => {
      const response = await api.datasets.getExamples(datasetId, configName, revision);
      return response.data;
    },
    enabled: enabled && !!datasetId,
    staleTime: 1000 * 60 * 30, // 30 minutes
  });
};

export const useCacheDataset = () => {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async (request: DatasetDownloadRequest) => {
      const response = await api.datasets.cache(request);
      return response.data;
    },
    onSuccess: () => {
      // Invalidate datasets queries to refresh the list
      queryClient.invalidateQueries({ queryKey: ['datasets'] });
    },
    onError: (error) => {
      console.error('Failed to cache dataset:', error);
    },
  });
};

export const useDownloadDatasetZip = () => {
  return useMutation({
    mutationFn: async ({ 
      datasetId, 
      configName, 
      revision 
    }: { 
      datasetId: string; 
      configName?: string; 
      revision?: string; 
    }) => {
      const response = await api.datasets.downloadZip(datasetId, configName, revision);
      
      // Create a download link and trigger download
      const blob = new Blob([response.data], { type: 'application/zip' });
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      
      // Generate filename
      const safeDatasetId = datasetId.replace('/', '_');
      const configSuffix = configName ? `_${configName}` : '';
      const revisionSuffix = revision ? `_${revision}` : '';
      link.download = `${safeDatasetId}${configSuffix}${revisionSuffix}.zip`;
      
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);
      
      return response.data;
    },
    onError: (error) => {
      console.error('Failed to download dataset ZIP:', error);
    },
  });
}; 