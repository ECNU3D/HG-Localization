import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useEffect } from 'react';
import { api } from '../api/client';
import { S3Config, ConfigStatus, DefaultConfig } from '../types';

export const useConfigStatus = () => {
  const queryClient = useQueryClient();
  
  const query = useQuery({
    queryKey: ['config', 'status'],
    queryFn: async () => {
      const response = await api.config.getStatus();
      return response.data;
    },
    staleTime: 1000 * 60 * 5, // 5 minutes
  });

  // Listen for authentication errors and refresh config status
  useEffect(() => {
    const handleAuthError = (event: CustomEvent) => {
      console.log('Auth error detected, refreshing config status...', event.detail);
      // Invalidate and refetch the config status
      queryClient.invalidateQueries({ queryKey: ['config', 'status'] });
    };

    window.addEventListener('auth-error', handleAuthError as EventListener);
    
    return () => {
      window.removeEventListener('auth-error', handleAuthError as EventListener);
    };
  }, [queryClient]);

  return query;
};

export const useDefaultConfig = () => {
  return useQuery({
    queryKey: ['config', 'defaults'],
    queryFn: async () => {
      const response = await api.config.getDefaults();
      return response.data;
    },
    staleTime: 1000 * 60 * 60, // 1 hour - defaults don't change often
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

export const useClearConfig = () => {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async () => {
      const response = await api.config.clearConfig();
      return response.data;
    },
    onSuccess: () => {
      // Clear the config status cache and set to unconfigured state
      queryClient.setQueryData(['config', 'status'], {
        configured: false,
        has_credentials: false,
        credentials_valid: false,
        bucket_name: null,
        endpoint_url: null,
        data_prefix: null,
      });
      // Invalidate datasets and models queries since config changed
      queryClient.invalidateQueries({ queryKey: ['datasets'] });
      queryClient.invalidateQueries({ queryKey: ['models'] });
    },
    onError: (error) => {
      console.error('Failed to clear configuration:', error);
    },
  });
}; 