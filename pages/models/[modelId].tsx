import React from 'react';
import { Layout } from '../../src/components/Layout';
import { ModelDetailPage } from '../../src/pages/ModelDetailPage';

export default function ModelDetailPageWrapper() {
  return (
    <Layout>
      <ModelDetailPage />
    </Layout>
  );
} 