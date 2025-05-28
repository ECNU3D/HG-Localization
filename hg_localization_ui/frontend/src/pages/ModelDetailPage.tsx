import React, { useState } from 'react';
import { useParams, useSearchParams, Link } from 'react-router-dom';
import { 
  ArrowLeft, 
  Brain, 
  FileText, 
  Settings, 
  Code, 
  ExternalLink,
  CheckCircle,
  XCircle,
  Copy,
  Check,
  Loader2,
  AlertCircle
} from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import Editor from '@monaco-editor/react';
import { useModelCard, useModelConfig, useModelExamples } from '../hooks/useModels';
import { CodeExample } from '../types';

// Copy button component (same as dataset page)
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
      className="json-pre font-mono text-sm whitespace-pre-wrap"
      dangerouslySetInnerHTML={{ __html: highlightJson(json) }}
    />
  );
};

export const ModelDetailPage: React.FC = () => {
  const { modelId } = useParams<{ modelId: string }>();
  const [searchParams] = useSearchParams();
  const revision = searchParams.get('revision') || undefined;
  
  const [activeTab, setActiveTab] = useState<'card' | 'config' | 'examples'>('card');

  const decodedModelId = modelId ? decodeURIComponent(modelId) : '';
  
  const { 
    data: modelCard, 
    isLoading: cardLoading, 
    error: cardError 
  } = useModelCard(decodedModelId, revision);
  
  const { 
    data: modelConfig, 
    isLoading: configLoading, 
    error: configError 
  } = useModelConfig(decodedModelId, revision);
  
  const { 
    data: examples, 
    isLoading: examplesLoading, 
    error: examplesError 
  } = useModelExamples(decodedModelId, revision);

  const getHuggingFaceUrl = () => {
    const baseUrl = `https://huggingface.co/${decodedModelId}`;
    return revision && revision !== 'main' ? `${baseUrl}/tree/${revision}` : baseUrl;
  };

  const tabs = [
    { id: 'card' as const, name: 'Model Card', icon: FileText, available: !!modelCard },
    { id: 'config' as const, name: 'Configuration', icon: Settings, available: !!modelConfig },
    { id: 'examples' as const, name: 'Code Examples', icon: Code, available: !!examples },
  ];

  const renderCard = () => {
    if (cardLoading) {
      return (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="w-8 h-8 text-primary-600 animate-spin" />
        </div>
      );
    }

    if (cardError || !modelCard) {
      return (
        <div className="text-center py-12">
          <AlertCircle className="w-12 h-12 text-gray-400 mx-auto mb-4" />
          <h3 className="text-lg font-medium text-gray-900 mb-2">Model Card Not Available</h3>
          <p className="text-gray-600">
            The model card could not be loaded. It may not exist or there was an error fetching it.
          </p>
        </div>
      );
    }

    return (
      <div className="prose max-w-none">
        <ReactMarkdown>{modelCard.content}</ReactMarkdown>
      </div>
    );
  };

  const renderConfig = () => {
    if (configLoading) {
      return (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="w-8 h-8 text-primary-600 animate-spin" />
        </div>
      );
    }

    if (configError || !modelConfig) {
      return (
        <div className="text-center py-12">
          <AlertCircle className="w-12 h-12 text-gray-400 mx-auto mb-4" />
          <h3 className="text-lg font-medium text-gray-900 mb-2">Configuration Not Available</h3>
          <p className="text-gray-600">
            The model configuration could not be loaded. It may not exist or there was an error fetching it.
          </p>
        </div>
      );
    }

    return (
      <div className="space-y-6">
        {/* Key Configuration Details */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {modelConfig.config.model_type && (
            <div className="bg-gray-50 p-4 rounded-lg">
              <h4 className="font-medium text-gray-900">Model Type</h4>
              <p className="text-gray-600">{modelConfig.config.model_type}</p>
            </div>
          )}
          {modelConfig.config.architectures && (
            <div className="bg-gray-50 p-4 rounded-lg">
              <h4 className="font-medium text-gray-900">Architecture</h4>
              <p className="text-gray-600">{modelConfig.config.architectures.join(', ')}</p>
            </div>
          )}
          {modelConfig.config.hidden_size && (
            <div className="bg-gray-50 p-4 rounded-lg">
              <h4 className="font-medium text-gray-900">Hidden Size</h4>
              <p className="text-gray-600">{modelConfig.config.hidden_size}</p>
            </div>
          )}
          {modelConfig.config.num_attention_heads && (
            <div className="bg-gray-50 p-4 rounded-lg">
              <h4 className="font-medium text-gray-900">Attention Heads</h4>
              <p className="text-gray-600">{modelConfig.config.num_attention_heads}</p>
            </div>
          )}
          {modelConfig.config.num_hidden_layers && (
            <div className="bg-gray-50 p-4 rounded-lg">
              <h4 className="font-medium text-gray-900">Hidden Layers</h4>
              <p className="text-gray-600">{modelConfig.config.num_hidden_layers}</p>
            </div>
          )}
          {modelConfig.config.vocab_size && (
            <div className="bg-gray-50 p-4 rounded-lg">
              <h4 className="font-medium text-gray-900">Vocabulary Size</h4>
              <p className="text-gray-600">{modelConfig.config.vocab_size.toLocaleString()}</p>
            </div>
          )}
        </div>

        {/* Full Configuration JSON */}
        <div>
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-medium text-gray-900">Full Configuration</h3>
            <CopyButton text={JSON.stringify(modelConfig.config, null, 2)} />
          </div>
          <JsonHighlighter json={JSON.stringify(modelConfig.config, null, 2)} />
        </div>
      </div>
    );
  };

  const renderExamples = () => {
    if (examplesLoading) {
      return (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="w-8 h-8 text-primary-600 animate-spin" />
        </div>
      );
    }

    if (examplesError || !examples || examples.length === 0) {
      return (
        <div className="text-center py-12">
          <Code className="w-12 h-12 text-gray-400 mx-auto mb-4" />
          <h3 className="text-lg font-medium text-gray-900 mb-2">No Examples Available</h3>
          <p className="text-gray-600">
            Code examples could not be generated for this model.
          </p>
        </div>
      );
    }

    return (
      <div className="space-y-6">
        {examples.map((example: CodeExample, index: number) => (
          <div key={index} className="bg-white border border-gray-200 rounded-lg overflow-hidden">
            <div className="bg-gray-50 px-4 py-3 border-b border-gray-200">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-lg font-medium text-gray-900">{example.title}</h3>
                  <p className="text-sm text-gray-600">{example.description}</p>
                </div>
                <CopyButton text={example.code} />
              </div>
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
        ))}
      </div>
    );
  };

  if (!decodedModelId) {
    return (
      <div className="text-center py-12">
        <AlertCircle className="w-12 h-12 text-red-500 mx-auto mb-4" />
        <h3 className="text-lg font-medium text-gray-900 mb-2">Invalid Model ID</h3>
        <p className="text-gray-600">The model ID in the URL is invalid.</p>
        <Link
          to="/models"
          className="inline-flex items-center mt-4 px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700"
        >
          <ArrowLeft className="w-4 h-4 mr-2" />
          Back to Models
        </Link>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <Link
            to="/models"
            className="inline-flex items-center text-sm font-medium text-gray-500 hover:text-gray-700"
          >
            <ArrowLeft className="w-4 h-4 mr-1" />
            Back to Models
          </Link>
        </div>
        <a
          href={getHuggingFaceUrl()}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50"
        >
          <ExternalLink className="w-4 h-4 mr-2" />
          View on Hugging Face
        </a>
      </div>

      {/* Model Info */}
      <div className="bg-white shadow rounded-lg p-6">
        <div className="flex items-start space-x-4">
          <Brain className="w-8 h-8 text-primary-600 mt-1" />
          <div className="flex-1">
            <h1 className="text-2xl font-bold text-gray-900">{decodedModelId}</h1>
            {revision && revision !== 'main' && (
              <p className="text-sm text-gray-600 mt-1">Revision: {revision}</p>
            )}
            <div className="flex items-center space-x-6 mt-4 text-sm text-gray-500">
              <div className="flex items-center space-x-1">
                {modelCard ? (
                  <CheckCircle className="w-4 h-4 text-green-500" />
                ) : (
                  <XCircle className="w-4 h-4 text-red-500" />
                )}
                <span>Model Card</span>
              </div>
              <div className="flex items-center space-x-1">
                {modelConfig ? (
                  <CheckCircle className="w-4 h-4 text-green-500" />
                ) : (
                  <XCircle className="w-4 h-4 text-red-500" />
                )}
                <span>Configuration</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="bg-white shadow rounded-lg">
        <div className="border-b border-gray-200">
          <nav className="-mb-px flex space-x-8 px-6">
            {tabs.map((tab) => {
              const Icon = tab.icon;
              return (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={`py-4 px-1 border-b-2 font-medium text-sm flex items-center space-x-2 ${
                    activeTab === tab.id
                      ? 'border-primary-500 text-primary-600'
                      : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                  } ${!tab.available ? 'opacity-50 cursor-not-allowed' : ''}`}
                  disabled={!tab.available}
                >
                  <Icon className="w-4 h-4" />
                  <span>{tab.name}</span>
                  {!tab.available && <XCircle className="w-4 h-4 text-red-500" />}
                </button>
              );
            })}
          </nav>
        </div>

        <div className="p-6">
          {activeTab === 'card' && renderCard()}
          {activeTab === 'config' && renderConfig()}
          {activeTab === 'examples' && renderExamples()}
        </div>
      </div>
    </div>
  );
}; 