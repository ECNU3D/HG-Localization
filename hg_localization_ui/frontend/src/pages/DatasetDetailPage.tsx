import React, { useState } from 'react';
import { useParams, useSearchParams, Link } from 'react-router-dom';
import { ArrowLeft, Eye, FileText, Code, Download, ExternalLink, ChevronDown, ChevronRight, Copy, Check } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import Editor from '@monaco-editor/react';
import { 
  useDatasetPreview, 
  useDatasetCard, 
  useDatasetExamples,
  useDownloadDatasetZip 
} from '../hooks/useDatasets';

// Copy button component
const CopyButton: React.FC<{ text: string; className?: string }> = ({ text, className = "" }) => {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000); // Reset after 2 seconds
    } catch (err) {
      console.error('Failed to copy text: ', err);
      // Fallback for older browsers
      const textArea = document.createElement('textarea');
      textArea.value = text;
      document.body.appendChild(textArea);
      textArea.select();
      document.execCommand('copy');
      document.body.removeChild(textArea);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  return (
    <button
      onClick={handleCopy}
      className={`flex items-center space-x-1 px-2 py-1 text-sm text-gray-600 hover:text-gray-800 hover:bg-gray-100 rounded transition-colors ${className}`}
      title={copied ? "Copied!" : "Copy code"}
    >
      {copied ? (
        <>
          <Check className="w-4 h-4 text-green-600" />
          <span className="text-green-600">Copied!</span>
        </>
      ) : (
        <>
          <Copy className="w-4 h-4" />
          <span>Copy</span>
        </>
      )}
    </button>
  );
};

// Simple JSON syntax highlighter
const JsonHighlighter: React.FC<{ json: string }> = ({ json }) => {
  const highlightJson = (jsonStr: string) => {
    return jsonStr
      .replace(/"([^"]+)":/g, '<span class="json-key">"$1":</span>')
      .replace(/"([^"]*)"(?=\s*[,\]}])/g, '<span class="json-string">"$1"</span>')
      .replace(/:\s*(-?\d+\.?\d*)/g, ': <span class="json-number">$1</span>')
      .replace(/:\s*(true|false)/g, ': <span class="json-boolean">$1</span>')
      .replace(/:\s*null/g, ': <span class="json-null">null</span>');
  };

  return (
    <pre 
      className="json-pre font-mono text-xs whitespace-pre-wrap"
      dangerouslySetInnerHTML={{ __html: highlightJson(json) }}
    />
  );
};

// Cell Renderer Component for table data
const CellRenderer: React.FC<{ value: any }> = ({ value }) => {
  const [isExpanded, setIsExpanded] = useState(false);
  
  if (value === null || value === undefined) {
    return <span className="text-gray-400 italic">null</span>;
  }
  
  if (typeof value !== 'object') {
    const stringValue = String(value);
    if (stringValue.length > 100) {
      return (
        <div className="space-y-1">
          <div className="max-w-xs">
            {isExpanded ? stringValue : `${stringValue.substring(0, 100)}...`}
          </div>
          <button
            onClick={() => setIsExpanded(!isExpanded)}
            className="text-xs text-blue-600 hover:text-blue-800"
          >
            {isExpanded ? 'Show less' : 'Show more'}
          </button>
        </div>
      );
    }
    return <span className="max-w-xs break-words">{stringValue}</span>;
  }
  
  const jsonString = JSON.stringify(value, null, 2);
  const isComplex = jsonString.length > 50;
  
  if (!isComplex) {
    return <span className="font-mono text-xs">{jsonString}</span>;
  }
  
  return (
    <div className="space-y-1">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="flex items-center space-x-1 text-blue-600 hover:text-blue-800 text-xs"
      >
        {isExpanded ? (
          <ChevronDown className="w-3 h-3" />
        ) : (
          <ChevronRight className="w-3 h-3" />
        )}
        <span>
          {Array.isArray(value) ? `Array (${value.length})` : `Object (${Object.keys(value).length})`}
        </span>
      </button>
      
      {isExpanded && (
        <div className="max-h-32 overflow-auto">
          <JsonHighlighter json={jsonString} />
        </div>
      )}
    </div>
  );
};

// JSON Viewer Component for better feature display
const JsonViewer: React.FC<{ data: any; name: string }> = ({ data, name }) => {
  const [isExpanded, setIsExpanded] = useState(false);
  
  if (typeof data !== 'object' || data === null) {
    return (
      <div className="flex justify-between items-center">
        <span className="text-gray-600 truncate">{name}:</span>
        <span className="font-mono text-sm text-gray-800 ml-2">
          {String(data)}
        </span>
      </div>
    );
  }

  const jsonString = JSON.stringify(data, null, 2);
  const isComplex = jsonString.length > 50 || jsonString.includes('\n');

  if (!isComplex) {
    return (
      <div className="flex justify-between items-center">
        <span className="text-gray-600 truncate">{name}:</span>
        <span className="font-mono text-sm text-gray-800 ml-2">
          {jsonString}
        </span>
      </div>
    );
  }

  return (
    <div className="space-y-2">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="flex items-center space-x-1 text-gray-600 hover:text-gray-800 transition-colors"
      >
        {isExpanded ? (
          <ChevronDown className="w-4 h-4" />
        ) : (
          <ChevronRight className="w-4 h-4" />
        )}
        <span className="truncate">{name}:</span>
        {!isExpanded && (
          <span className="font-mono text-xs text-gray-500 ml-2">
            {Object.keys(data).length} properties
          </span>
        )}
      </button>
      
      {isExpanded && (
        <div className="ml-4 border border-gray-200 rounded-lg overflow-hidden">
          <Editor
            height="120px"
            language="json"
            value={jsonString}
            options={{
              readOnly: true,
              minimap: { enabled: false },
              scrollBeyondLastLine: false,
              fontSize: 12,
              lineNumbers: 'off',
              theme: 'vs-light',
              wordWrap: 'on',
              folding: false,
              glyphMargin: false,
              lineDecorationsWidth: 0,
              lineNumbersMinChars: 0,
              automaticLayout: true,
              scrollbar: {
                vertical: 'auto',
                horizontal: 'auto',
                verticalScrollbarSize: 8,
                horizontalScrollbarSize: 8,
              },
            }}
            onMount={(editor) => {
              // Force layout after a short delay to prevent ResizeObserver errors
              setTimeout(() => {
                editor.layout();
              }, 100);
            }}
          />
        </div>
      )}
    </div>
  );
};

export const DatasetDetailPage: React.FC = () => {
  const { datasetId } = useParams<{ datasetId: string }>();
  const [searchParams] = useSearchParams();
  const configName = searchParams.get('config') || undefined;
  const revision = searchParams.get('revision') || undefined;
  
  const [activeTab, setActiveTab] = useState<'preview' | 'card' | 'examples'>('preview');
  
  const downloadZipMutation = useDownloadDatasetZip();

  const { 
    data: preview, 
    isLoading: previewLoading, 
    error: previewError 
  } = useDatasetPreview(datasetId!, configName, revision, activeTab === 'preview');

  const { 
    data: card, 
    isLoading: cardLoading, 
    error: cardError 
  } = useDatasetCard(datasetId!, configName, revision, activeTab === 'card');

  const { 
    data: examples, 
    isLoading: examplesLoading 
  } = useDatasetExamples(datasetId!, configName, revision, activeTab === 'examples');

  const handleDownload = async () => {
    if (!datasetId) return;
    
    try {
      await downloadZipMutation.mutateAsync({
        datasetId: datasetId,
        configName: configName,
        revision: revision,
      });
    } catch (error) {
      console.error('Download failed:', error);
    }
  };

  if (!datasetId) {
    return (
      <div className="text-center py-12">
        <p className="text-red-600">Dataset ID is required</p>
      </div>
    );
  }

  const tabs = [
    { id: 'preview', label: 'Preview', icon: Eye },
    { id: 'card', label: 'Dataset Card', icon: FileText },
    { id: 'examples', label: 'Code Examples', icon: Code },
  ] as const;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <Link
            to="/datasets"
            className="btn-outline flex items-center space-x-2"
          >
            <ArrowLeft className="w-4 h-4" />
            <span>Back to Datasets</span>
          </Link>
          
          <div>
            <h1 className="text-3xl font-bold text-gray-900">{datasetId}</h1>
            <div className="flex items-center space-x-4 mt-2 text-sm text-gray-600">
              {configName && (
                <span>
                  <strong>Config:</strong> {configName}
                </span>
              )}
              {revision && (
                <span>
                  <strong>Revision:</strong> {revision}
                </span>
              )}
            </div>
          </div>
        </div>

        <div className="flex items-center space-x-3">
          <a
            href={`https://huggingface.co/datasets/${datasetId.replace(/_/g, '/')}`}
            target="_blank"
            rel="noopener noreferrer"
            className="btn-outline flex items-center space-x-2"
          >
            <ExternalLink className="w-4 h-4" />
            <span>View on HF Hub</span>
          </a>
          
          <button
            onClick={handleDownload}
            disabled={downloadZipMutation.isPending}
            className="btn-primary flex items-center space-x-2 disabled:opacity-50"
            title="Download dataset ZIP to your computer"
          >
            {downloadZipMutation.isPending ? (
              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
            ) : (
              <Download className="w-4 h-4" />
            )}
            <span>
              {downloadZipMutation.isPending ? 'Downloading...' : 'Download ZIP'}
            </span>
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div className="border-b border-gray-200">
        <nav className="-mb-px flex space-x-8">
          {tabs.map((tab) => {
            const Icon = tab.icon;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`py-2 px-1 border-b-2 font-medium text-sm flex items-center space-x-2 transition-colors ${
                  activeTab === tab.id
                    ? 'border-primary-500 text-primary-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                <Icon className="w-4 h-4" />
                <span>{tab.label}</span>
              </button>
            );
          })}
        </nav>
      </div>

      {/* Tab Content */}
      <div className="min-h-96">
        {activeTab === 'preview' && (
          <div className="space-y-6">
            {previewLoading ? (
              <div className="flex items-center justify-center py-12">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
              </div>
            ) : previewError ? (
              <div className="card bg-red-50 border-red-200">
                <p className="text-red-800">
                  Failed to load dataset preview. The dataset might not be available locally.
                </p>
              </div>
            ) : preview ? (
              <>
                {/* Dataset Info */}
                <div className="card">
                  <h2 className="text-lg font-semibold text-gray-900 mb-4">Dataset Information</h2>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <h3 className="font-medium text-gray-700 mb-2">Splits</h3>
                      <div className="space-y-1">
                        {Object.entries(preview.num_rows).map(([split, count]) => (
                          <div key={split} className="flex justify-between">
                            <span className="text-gray-600">{split}:</span>
                            <span className="font-medium">{count.toLocaleString()} rows</span>
                          </div>
                        ))}
                      </div>
                    </div>
                    
                    <div>
                      <h3 className="font-medium text-gray-700 mb-2">Features</h3>
                      <div className="space-y-1">
                        {Object.entries(preview.features).map(([name, type]) => (
                          <JsonViewer key={name} data={type} name={name} />
                        ))}
                      </div>
                    </div>
                  </div>
                </div>

                {/* Sample Data */}
                <div className="card">
                  <h2 className="text-lg font-semibold text-gray-900 mb-4">Sample Data</h2>
                  <div className="overflow-x-auto">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50">
                        <tr>
                          {Object.keys(preview.features).map((feature) => (
                            <th
                              key={feature}
                              className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                            >
                              {feature}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {preview.sample_data.map((row, index) => (
                          <tr key={index}>
                            {Object.keys(preview.features).map((feature) => (
                              <td
                                key={feature}
                                className="px-6 py-4 text-sm text-gray-900"
                              >
                                <CellRenderer value={row[feature]} />
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </>
            ) : (
              <div className="card text-center py-12">
                <p className="text-gray-600">No preview available</p>
              </div>
            )}
          </div>
        )}

        {activeTab === 'card' && (
          <div className="card">
            {cardLoading ? (
              <div className="flex items-center justify-center py-12">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
              </div>
            ) : cardError ? (
              <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                <p className="text-yellow-800">
                  Dataset card not available. This might be a private dataset or the card hasn't been created yet.
                </p>
              </div>
            ) : card ? (
              <div className="prose max-w-none">
                <ReactMarkdown remarkPlugins={[remarkGfm]}>{card.content}</ReactMarkdown>
              </div>
            ) : (
              <div className="text-center py-12">
                <FileText className="w-12 h-12 text-gray-400 mx-auto mb-4" />
                <p className="text-gray-600">No dataset card available</p>
              </div>
            )}
          </div>
        )}

        {activeTab === 'examples' && (
          <div className="space-y-6">
            {examplesLoading ? (
              <div className="flex items-center justify-center py-12">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
              </div>
            ) : examples && examples.length > 0 ? (
              examples.map((example, index) => (
                <div key={index} className="card">
                  <div className="flex items-center justify-between mb-4">
                    <div>
                      <h3 className="text-lg font-semibold text-gray-900">{example.title}</h3>
                      <p className="text-gray-600">{example.description}</p>
                    </div>
                    <CopyButton text={example.code} />
                  </div>
                  
                  <div className="border border-gray-200 rounded-lg overflow-hidden relative">
                    <Editor
                      height="200px"
                      language={example.language}
                      value={example.code}
                      options={{
                        readOnly: true,
                        minimap: { enabled: false },
                        scrollBeyondLastLine: false,
                        fontSize: 14,
                        lineNumbers: 'on',
                        theme: 'vs-light',
                      }}
                    />
                    {/* Additional copy button in the top-right corner of the code editor */}
                    <div className="absolute top-2 right-2">
                      <CopyButton 
                        text={example.code} 
                        className="bg-white/90 backdrop-blur-sm border border-gray-200 shadow-sm"
                      />
                    </div>
                  </div>
                </div>
              ))
            ) : (
              <div className="card text-center py-12">
                <Code className="w-12 h-12 text-gray-400 mx-auto mb-4" />
                <p className="text-gray-600">No code examples available</p>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}; 