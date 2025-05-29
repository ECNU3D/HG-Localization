import { useQuery, useMutation } from '@tanstack/react-query';
import { api } from '../api/client';
import { ModelTestingConfig, ModelTestRequest, ModelTestResponse, ModelAvailabilityCheck } from '../types';

// Query keys
export const modelTestingKeys = {
  all: ['model-testing'] as const,
  config: () => [...modelTestingKeys.all, 'config'] as const,
  availability: (modelId: string, apiKey: string) => [...modelTestingKeys.all, 'availability', modelId, apiKey] as const,
};

// Hook for fetching model testing configuration
export const useModelTestingConfig = () => {
  return useQuery({
    queryKey: modelTestingKeys.config(),
    queryFn: async (): Promise<ModelTestingConfig> => {
      const response = await api.modelTesting.getConfig();
      return response.data;
    },
    staleTime: 5 * 60 * 1000, // 5 minutes
    retry: false, // Don't retry if feature is disabled
  });
};

// Hook for checking model availability (ping test)
export const useModelAvailability = (modelId: string, apiKey: string, enabled: boolean = true) => {
  return useQuery({
    queryKey: modelTestingKeys.availability(modelId, apiKey),
    queryFn: async (): Promise<ModelAvailabilityCheck> => {
      const response = await api.modelTesting.checkAvailability(modelId, apiKey);
      return response.data;
    },
    enabled: enabled && !!modelId && !!apiKey,
    staleTime: 2 * 60 * 1000, // 2 minutes
    retry: (failureCount, error: any) => {
      // Don't retry if it's a feature disabled error
      if (error?.response?.status === 404) return false;
      return failureCount < 2;
    },
  });
};

// Hook for testing model with prompt
export const useTestModel = () => {
  return useMutation({
    mutationFn: async (request: ModelTestRequest): Promise<ModelTestResponse> => {
      const response = await api.modelTesting.testModel(request);
      return response.data;
    },
    retry: false, // Don't retry failed model tests
  });
};

// Helper function to check if model testing is available
export const useIsModelTestingAvailable = () => {
  const { data: config, isLoading, error } = useModelTestingConfig();
  
  return {
    isAvailable: config?.enabled ?? false,
    isLoading,
    hasError: !!error,
    error: error as any,
    config,
  };
};

// Helper function to get availability status display
export const getAvailabilityStatusDisplay = (availability: ModelAvailabilityCheck): {
  status: 'available' | 'unavailable' | 'unknown';
  message: string;
  color: string;
} => {
  if (availability.available) {
    return {
      status: 'available',
      message: 'Model is available and ready for testing',
      color: 'green',
    };
  } else if (availability.error) {
    return {
      status: 'unavailable',
      message: availability.error,
      color: 'red',
    };
  } else {
    return {
      status: 'unknown',
      message: 'Model availability unknown',
      color: 'gray',
    };
  }
};

// Helper function to get test response display
export const getTestResponseDisplay = (response: ModelTestResponse): {
  status: 'success' | 'error';
  message: string;
  color: string;
} => {
  if (response.success && response.response) {
    return {
      status: 'success',
      message: response.response,
      color: 'green',
    };
  } else if (response.error) {
    return {
      status: 'error',
      message: response.error,
      color: 'red',
    };
  } else {
    return {
      status: 'error',
      message: 'Unknown error occurred',
      color: 'red',
    };
  }
}; 