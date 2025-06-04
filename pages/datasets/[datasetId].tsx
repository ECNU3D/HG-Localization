import React from 'react';
import { Layout } from '../../src/components/Layout';
import { DatasetDetailPage } from '../../src/pages/DatasetDetailPage';

export default function DatasetDetailPageWrapper() {
  return (
    <Layout>
      <DatasetDetailPage />
    </Layout>
  );
} 