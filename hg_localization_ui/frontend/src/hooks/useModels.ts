import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { api } from '../api/client';
import { ModelInfo, ModelDownloadRequest, ModelCard, ModelConfig, CodeExample } from '../types';

// Query keys
export const modelKeys = {
  all: ['models'] as const,
  cached: () => [...modelKeys.all, 'cached'] as const,
  s3: () => [...modelKeys.all, 's3'] as const,
  allModels: () => [...modelKeys.all, 'allModels'] as const,
  detail: (modelId: string, revision?: string) => [...modelKeys.all, 'detail', modelId, revision] as const,
  card: (modelId: string, revision?: string) => [...modelKeys.all, 'card', modelId, revision] as const,
  config: (modelId: string, revision?: string) => [...modelKeys.all, 'config', modelId, revision] as const,
  examples: (modelId: string, revision?: string) => [...modelKeys.all, 'examples', modelId, revision] as const,
};

// Hooks for fetching models
export const useCachedModels = () => {
  return useQuery({
    queryKey: modelKeys.cached(),
    queryFn: async (): Promise<ModelInfo[]> => {
      const response = await api.models.getCached();
      return response.data;
    },
    staleTime: 30 * 1000, // 30 seconds
  });
};

export const useS3Models = () => {
  return useQuery({
    queryKey: modelKeys.s3(),
    queryFn: async (): Promise<ModelInfo[]> => {
      const response = await api.models.getS3();
      return response.data;
    },
    staleTime: 30 * 1000, // 30 seconds
  });
};

export const useModels = () => {
  return useQuery({
    queryKey: modelKeys.allModels(),
    queryFn: async (): Promise<ModelInfo[]> => {
      const response = await api.models.getAll();
      return response.data;
    },
    staleTime: 30 * 1000, // 30 seconds
  });
};

// Hook for caching models
export const useCacheModel = () => {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async (request: ModelDownloadRequest) => {
      const response = await api.models.cache(request);
      return response.data;
    },
    onSuccess: () => {
      // Invalidate all model queries to refresh the lists
      queryClient.invalidateQueries({ queryKey: modelKeys.cached() });
      queryClient.invalidateQueries({ queryKey: modelKeys.s3() });
      queryClient.invalidateQueries({ queryKey: modelKeys.allModels() });
    },
  });
};

// Hook for fetching model card
export const useModelCard = (modelId: string, revision?: string, enabled: boolean = true) => {
  return useQuery({
    queryKey: modelKeys.card(modelId, revision),
    queryFn: async (): Promise<ModelCard> => {
      const response = await api.models.getCard(modelId, revision);
      return response.data;
    },
    enabled: enabled && !!modelId,
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
};

// Hook for fetching model config
export const useModelConfig = (modelId: string, revision?: string, enabled: boolean = true) => {
  return useQuery({
    queryKey: modelKeys.config(modelId, revision),
    queryFn: async (): Promise<ModelConfig> => {
      const response = await api.models.getConfig(modelId, revision);
      return response.data;
    },
    enabled: enabled && !!modelId,
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
};

// Hook for fetching model examples
export const useModelExamples = (modelId: string, revision?: string, enabled: boolean = true) => {
  return useQuery({
    queryKey: modelKeys.examples(modelId, revision),
    queryFn: async (): Promise<CodeExample[]> => {
      const response = await api.models.getExamples(modelId, revision);
      return response.data;
    },
    enabled: enabled && !!modelId,
    staleTime: 10 * 60 * 1000, // 10 minutes
  });
};

// Helper function to get model display name
export const getModelDisplayName = (model: ModelInfo): string => {
  const revision = model.revision && model.revision !== 'default' ? ` (${model.revision})` : '';
  return `${model.model_id}${revision}`;
};

// Helper function to get model type display
export const getModelTypeDisplay = (model: ModelInfo): string => {
  return model.is_full_model ? 'Full Model' : 'Metadata Only';
};

// Helper function to get model status color
export const getModelStatusColor = (model: ModelInfo): string => {
  if (model.is_full_model) return 'green';
  return 'blue';
}; 