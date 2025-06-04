import React, { useEffect } from 'react';
import { useRouter } from 'next/router';
import { Layout } from '../src/components/Layout';
import { useConfigStatus } from '../src/hooks/useConfig';

export default function HomePage() {
  const router = useRouter();
  const { data: configStatus, isLoading } = useConfigStatus();

  useEffect(() => {
    if (!isLoading && configStatus) {
      if (configStatus.configured) {
        router.replace('/datasets');
      } else {
        router.replace('/config');
      }
    }
  }, [configStatus, isLoading, router]);

  if (isLoading) {
    return (
      <Layout>
        <div className="min-h-screen flex items-center justify-center">
          <div className="text-center">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary-600 mx-auto mb-4"></div>
            <p className="text-gray-600">Loading configuration...</p>
          </div>
        </div>
      </Layout>
    );
  }

  return null;
} 