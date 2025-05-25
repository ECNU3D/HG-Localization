import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { api } from '../api/client';
import { S3Config, ConfigStatus } from '../types';

export const useConfigStatus = () => {
  return useQuery({
    queryKey: ['config', 'status'],
    queryFn: async () => {
      const response = await api.config.getStatus();
      return response.data;
    },
    staleTime: 1000 * 60 * 5, // 5 minutes
  });
};

export const useSetConfig = () => {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async (config: S3Config) => {
      const response = await api.config.setConfig(config);
      return response.data;
    },
    onSuccess: (data: ConfigStatus) => {
      // Update the config status cache
      queryClient.setQueryData(['config', 'status'], data);
      // Invalidate datasets queries since config changed
      queryClient.invalidateQueries({ queryKey: ['datasets'] });
    },
    onError: (error) => {
      console.error('Failed to set configuration:', error);
    },
  });
}; 