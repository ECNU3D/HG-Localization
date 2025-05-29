import React, { useState, useEffect } from 'react';
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
  AlertCircle,
  Play,
  Key,
  MessageSquare,
  Wifi,
  WifiOff,
  Upload,
  X
} from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import Editor from '@monaco-editor/react';
import { useModelCard, useModelConfig, useModelExamples } from '../hooks/useModels';
import { 
  useIsModelTestingAvailable, 
  useModelAvailability, 
  useTestModel,
  getAvailabilityStatusDisplay,
  getTestResponseDisplay
} from '../hooks/useModelTesting';
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
      className="json-pre font-mono text-base whitespace-pre-wrap"
      dangerouslySetInnerHTML={{ __html: highlightJson(json) }}
    />
  );
};

// Smart response formatter that detects content type and renders appropriately
const SmartResponseFormatter: React.FC<{ content: string; className?: string }> = ({ content, className = "" }) => {
  const detectContentType = (text: string): 'json' | 'markdown' | 'code' | 'text' => {
    const trimmed = text.trim();
    
    // Check for JSON
    if ((trimmed.startsWith('{') && trimmed.endsWith('}')) || 
        (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
      try {
        JSON.parse(trimmed);
        return 'json';
      } catch {
        // Not valid JSON, continue checking
      }
    }
    
    // Check for Markdown patterns
    const markdownPatterns = [
      /^#+\s/m,           // Headers
      /\*\*.*?\*\*/,      // Bold
      /\*.*?\*/,          // Italic
      /`.*?`/,            // Inline code
      /```[\s\S]*?```/,   // Code blocks
      /^\s*[-*+]\s/m,     // Lists
      /^\s*\d+\.\s/m,     // Numbered lists
      /\[.*?\]\(.*?\)/,   // Links
      /^\s*>\s/m,         // Blockquotes
    ];
    
    if (markdownPatterns.some(pattern => pattern.test(trimmed))) {
      return 'markdown';
    }
    
    // Check for code patterns
    const codePatterns = [
      /^\s*(?:function|const|let|var|class|import|export|def|if|for|while|return)\s/m,
      /^\s*(?:public|private|protected|static)\s/m,
      /\/\/.*$/m,         // Single line comments
      /\/\*[\s\S]*?\*\//, // Multi-line comments
      /#.*$/m,            // Python/shell comments
      /\{[\s\S]*\}/,      // Code blocks with braces
      /^\s*<[^>]+>/m,     // HTML/XML tags
    ];
    
    if (codePatterns.some(pattern => pattern.test(trimmed))) {
      return 'code';
    }
    
    return 'text';
  };

  const formatJsonForDisplay = (jsonStr: string): string => {
    try {
      const parsed = JSON.parse(jsonStr);
      return JSON.stringify(parsed, null, 2);
    } catch {
      return jsonStr;
    }
  };

  const contentType = detectContentType(content);

  const renderContent = () => {
    switch (contentType) {
      case 'json':
        return <JsonHighlighter json={formatJsonForDisplay(content)} />;
      
      case 'markdown':
        return (
          <div className="prose prose-base max-w-none text-gray-800">
            <ReactMarkdown remarkPlugins={[remarkGfm]}>{content}</ReactMarkdown>
          </div>
        );
      
      case 'code':
        return (
          <pre className="bg-gray-50 p-3 rounded-md overflow-x-auto border">
            <code className="font-mono text-base text-gray-800 whitespace-pre-wrap">
              {content}
            </code>
          </pre>
        );
      
      case 'text':
      default:
        return (
          <div className="whitespace-pre-wrap text-base text-gray-800 leading-relaxed">
            {content}
          </div>
        );
    }
  };

  return (
    <div className={`smart-response-formatter ${className}`}>
      {renderContent()}
    </div>
  );
};

export const ModelDetailPage: React.FC = () => {
  const { modelId } = useParams<{ modelId: string }>();
  const [searchParams] = useSearchParams();
  const revision = searchParams.get('revision') || undefined;
  
  const [activeTab, setActiveTab] = useState<'card' | 'config' | 'examples' | 'test'>('card');
  
  // Model testing state
  const [apiKey, setApiKey] = useState('');
  const [testMessage, setTestMessage] = useState('Hello! Can you help me with a simple question?');
  const [showApiKey, setShowApiKey] = useState(false);
  const [editableModelName, setEditableModelName] = useState('');

  // Image upload state
  const [uploadedImage, setUploadedImage] = useState<File | null>(null);
  const [imagePreview, setImagePreview] = useState<string | null>(null);
  const [dragOver, setDragOver] = useState(false);

  const decodedModelId = modelId ? decodeURIComponent(modelId) : '';
  
  // Update editable model name when the URL model ID changes
  useEffect(() => {
    setEditableModelName(decodedModelId);
  }, [decodedModelId]);
  
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

  // Model testing hooks
  const { isAvailable: modelTestingAvailable, isLoading: testingConfigLoading } = useIsModelTestingAvailable();
  
  const { 
    data: availability, 
    isLoading: availabilityLoading, 
    refetch: recheckAvailability 
  } = useModelAvailability(editableModelName, apiKey, modelTestingAvailable && !!apiKey && !!editableModelName);
  
  const {
    mutate: testModel,
    isPending: isTestingModel,
    data: testResponse,
    reset: resetTestResponse
  } = useTestModel();

  const getHuggingFaceUrl = () => {
    const baseUrl = `https://huggingface.co/${decodedModelId}`;
    return revision && revision !== 'main' ? `${baseUrl}/tree/${revision}` : baseUrl;
  };

  const tabs = [
    { id: 'card' as const, name: 'Model Card', icon: FileText, available: !!modelCard },
    { id: 'config' as const, name: 'Configuration', icon: Settings, available: !!modelConfig },
    { id: 'examples' as const, name: 'Code Examples', icon: Code, available: !!examples },
    { 
      id: 'test' as const, 
      name: 'Test Model', 
      icon: Play, 
      available: modelTestingAvailable,
      badge: availability?.available ? 'Ready' : (availability?.available === false ? 'Unavailable' : undefined)
    },
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
        <ReactMarkdown remarkPlugins={[remarkGfm]}>{modelCard.content}</ReactMarkdown>
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

  // Image upload handlers
  const handleImageUpload = (file: File) => {
    // Validate file type
    if (!file.type.match(/^image\/(png|jpeg|jpg)$/)) {
      alert('Please upload only PNG or JPEG images.');
      return;
    }

    // Validate file size (max 10MB)
    if (file.size > 10 * 1024 * 1024) {
      alert('Image must be smaller than 10MB.');
      return;
    }

    setUploadedImage(file);
    
    // Create preview
    const reader = new FileReader();
    reader.onload = (e) => {
      setImagePreview(e.target?.result as string);
    };
    reader.readAsDataURL(file);
  };

  const handleImageRemove = () => {
    setUploadedImage(null);
    setImagePreview(null);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    
    const files = Array.from(e.dataTransfer.files);
    if (files.length > 0) {
      handleImageUpload(files[0]);
    }
  };

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(true);
  };

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
  };

  const convertImageToBase64 = (file: File): Promise<string> => {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => {
        const result = reader.result as string;
        // Remove the data URL prefix to get just the base64 data
        const base64Data = result.split(',')[1];
        resolve(base64Data);
      };
      reader.onerror = reject;
      reader.readAsDataURL(file);
    });
  };

  const renderTestModel = () => {
    if (testingConfigLoading) {
      return (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="w-8 h-8 text-primary-600 animate-spin" />
        </div>
      );
    }

    if (!modelTestingAvailable) {
      return (
        <div className="text-center py-12">
          <AlertCircle className="w-12 h-12 text-gray-400 mx-auto mb-4" />
          <h3 className="text-lg font-medium text-gray-900 mb-2">Model Testing Not Available</h3>
          <p className="text-gray-600 mb-4">
            Model testing functionality is not enabled on this server.
          </p>
          <p className="text-sm text-gray-500">
            Contact your administrator to enable model testing functionality.
          </p>
        </div>
      );
    }

    const handleTestModel = async () => {
      if (!apiKey || !testMessage || !editableModelName) return;
      
      resetTestResponse();
      
      try {
        let imageData, imageType, imageFilename;
        
        if (uploadedImage) {
          imageData = await convertImageToBase64(uploadedImage);
          imageType = uploadedImage.type;
          imageFilename = uploadedImage.name;
        }
        
        testModel({
          model_id: editableModelName,
          api_key: apiKey,
          message: testMessage,
          image_data: imageData,
          image_type: imageType,
          image_filename: imageFilename
        });
      } catch (error) {
        console.error('Error processing image:', error);
        alert('Error processing image. Please try again.');
      }
    };

    const availabilityStatus = availability ? getAvailabilityStatusDisplay(availability) : null;
    const testResponseDisplay = testResponse ? getTestResponseDisplay(testResponse) : null;

    return (
      <div className="space-y-6">
        {/* API Key Configuration */}
        <div className="bg-white border border-gray-200 rounded-lg p-6">
          <div className="flex items-center mb-4">
            <Key className="w-5 h-5 text-gray-400 mr-2" />
            <h3 className="text-lg font-medium text-gray-900">API Configuration</h3>
          </div>
          
          <div className="space-y-4">
            {/* Model Name Input */}
            <div>
              <label htmlFor="model-name" className="block text-sm font-medium text-gray-700 mb-2">
                Model Name
              </label>
              <input
                id="model-name"
                type="text"
                value={editableModelName}
                onChange={(e) => setEditableModelName(e.target.value)}
                placeholder="Enter the model name as it appears in your OpenAI-compatible server"
                className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-primary-500 focus:border-primary-500"
              />
              <p className="text-sm text-gray-500 mt-1">
                This should match the exact model name in your OpenAI-compatible server. It may differ from the HuggingFace model ID.
                <br />
                <span className="text-xs">
                  Examples: <code>gpt-3.5-turbo</code>, <code>llama-2-7b-chat</code>, <code>mistral-7b-instruct</code>
                </span>
              </p>
            </div>

            {/* API Key Input */}
            <div>
              <label htmlFor="api-key" className="block text-sm font-medium text-gray-700 mb-2">
                API Key
              </label>
              <div className="relative">
                <input
                  id="api-key"
                  type={showApiKey ? "text" : "password"}
                  value={apiKey}
                  onChange={(e) => setApiKey(e.target.value)}
                  placeholder="Enter your API key"
                  className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-primary-500 focus:border-primary-500"
                />
                <button
                  type="button"
                  onClick={() => setShowApiKey(!showApiKey)}
                  className="absolute inset-y-0 right-0 flex items-center px-3 text-gray-400 hover:text-gray-600"
                >
                  {showApiKey ? (
                    <XCircle className="w-4 h-4" />
                  ) : (
                    <CheckCircle className="w-4 h-4" />
                  )}
                </button>
              </div>
              <p className="text-sm text-gray-500 mt-1">
                Your API key is only used for testing and is not stored on the server.
              </p>
            </div>

            {/* Model Availability Status */}
            {apiKey && editableModelName && (
              <div className="border-t pt-4">
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center">
                    {availabilityLoading ? (
                      <Loader2 className="w-4 h-4 text-gray-400 animate-spin mr-2" />
                    ) : availability?.available ? (
                      <Wifi className="w-4 h-4 text-green-500 mr-2" />
                    ) : (
                      <WifiOff className="w-4 h-4 text-red-500 mr-2" />
                    )}
                    <span className="text-sm font-medium text-gray-700">
                      Model Availability: <code className="text-xs bg-gray-100 px-1 py-0.5 rounded">{editableModelName}</code>
                    </span>
                  </div>
                  <button
                    onClick={() => recheckAvailability()}
                    disabled={availabilityLoading}
                    className="text-sm text-primary-600 hover:text-primary-700 disabled:opacity-50"
                  >
                    Recheck
                  </button>
                </div>
                
                {availabilityStatus && (
                  <div className={`p-3 rounded-md ${
                    availabilityStatus.color === 'green' 
                      ? 'bg-green-50 text-green-800' 
                      : availabilityStatus.color === 'red'
                      ? 'bg-red-50 text-red-800'
                      : 'bg-gray-50 text-gray-800'
                  }`}>
                    <p className="text-sm">{availabilityStatus.message}</p>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>

        {/* Test Interface */}
        {apiKey && editableModelName && availability?.available && (
          <div className="bg-white border border-gray-200 rounded-lg p-6">
            <div className="flex items-center mb-4">
              <MessageSquare className="w-5 h-5 text-gray-400 mr-2" />
              <h3 className="text-lg font-medium text-gray-900">Test Model</h3>
            </div>
            
            <div className="space-y-4">
              <div>
                <label htmlFor="test-message" className="block text-sm font-medium text-gray-700 mb-2">
                  Test Message
                </label>
                <textarea
                  id="test-message"
                  value={testMessage}
                  onChange={(e) => setTestMessage(e.target.value)}
                  rows={3}
                  className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-primary-500 focus:border-primary-500"
                  placeholder="Enter a message to test the model..."
                />
              </div>

              {/* Image Upload */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Image Upload (Optional)
                </label>
                <p className="text-xs text-gray-500 mb-3">
                  Upload an image for vision models. Supports PNG and JPEG formats up to 10MB.
                </p>
                
                {!uploadedImage ? (
                  <div
                    onDrop={handleDrop}
                    onDragOver={handleDragOver}
                    onDragLeave={handleDragLeave}
                    className={`border-2 border-dashed rounded-lg p-6 text-center cursor-pointer transition-colors ${
                      dragOver 
                        ? 'border-primary-400 bg-primary-50' 
                        : 'border-gray-300 hover:border-gray-400'
                    }`}
                  >
                    <input
                      type="file"
                      accept="image/png,image/jpeg,image/jpg"
                      onChange={(e) => {
                        const file = e.target.files?.[0];
                        if (file) handleImageUpload(file);
                      }}
                      className="hidden"
                      id="image-upload"
                    />
                    <label htmlFor="image-upload" className="cursor-pointer">
                      <Upload className="w-8 h-8 text-gray-400 mx-auto mb-2" />
                      <p className="text-sm text-gray-600 mb-1">
                        Drop an image here or click to browse
                      </p>
                      <p className="text-xs text-gray-500">
                        PNG, JPEG up to 10MB
                      </p>
                    </label>
                  </div>
                ) : (
                  <div className="border border-gray-300 rounded-lg p-4">
                    <div className="flex items-start space-x-4">
                      <div className="flex-shrink-0">
                        <img
                          src={imagePreview || ''}
                          alt="Uploaded preview"
                          className="w-20 h-20 object-cover rounded-lg border"
                        />
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center justify-between">
                          <div>
                            <p className="text-sm font-medium text-gray-900 truncate">
                              {uploadedImage.name}
                            </p>
                            <p className="text-xs text-gray-500">
                              {(uploadedImage.size / 1024 / 1024).toFixed(2)} MB • {uploadedImage.type}
                            </p>
                          </div>
                          <button
                            onClick={handleImageRemove}
                            className="ml-2 p-1 text-gray-400 hover:text-red-500 transition-colors"
                            title="Remove image"
                          >
                            <X className="w-4 h-4" />
                          </button>
                        </div>
                      </div>
                    </div>
                  </div>
                )}
              </div>

              <div className="flex justify-end">
                <button
                  onClick={handleTestModel}
                  disabled={!apiKey || !testMessage || !editableModelName || isTestingModel}
                  className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {isTestingModel ? (
                    <>
                      <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                      Testing...
                    </>
                  ) : (
                    <>
                      <Play className="w-4 h-4 mr-2" />
                      Test Model
                    </>
                  )}
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Test Response */}
        {testResponseDisplay && (
          <div className="bg-white border border-gray-200 rounded-lg p-6">
            <div className="flex items-center mb-4">
              <Brain className="w-5 h-5 text-gray-400 mr-2" />
              <h3 className="text-lg font-medium text-gray-900">Model Response</h3>
              {editableModelName && (
                <span className="ml-2 text-sm text-gray-500">
                  from <code className="text-xs bg-gray-100 px-1 py-0.5 rounded">{editableModelName}</code>
                </span>
              )}
            </div>
            
            <div className={`p-4 rounded-md ${
              testResponseDisplay.color === 'green' 
                ? 'bg-green-50 border border-green-200' 
                : 'bg-red-50 border border-red-200'
            }`}>
              <div className="flex items-start">
                {testResponseDisplay.status === 'success' ? (
                  <CheckCircle className="w-5 h-5 text-green-500 mr-3 mt-0.5 flex-shrink-0" />
                ) : (
                  <XCircle className="w-5 h-5 text-red-500 mr-3 mt-0.5 flex-shrink-0" />
                )}
                <div className="flex-1">
                  {testResponseDisplay.status === 'success' ? (
                    <SmartResponseFormatter 
                      content={testResponseDisplay.message}
                      className="text-green-800"
                    />
                  ) : (
                    <p className="text-sm text-red-800">
                      {testResponseDisplay.message}
                    </p>
                  )}
                </div>
                {testResponseDisplay.status === 'success' && (
                  <CopyButton 
                    text={testResponseDisplay.message} 
                    className="ml-2 flex-shrink-0"
                  />
                )}
              </div>
            </div>
          </div>
        )}
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
              const isTestTab = tab.id === 'test';
              const hasStatusBadge = isTestTab && tab.badge;
              
              return (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={`py-4 px-1 border-b-2 font-medium text-sm flex items-center space-x-2 relative ${
                    activeTab === tab.id
                      ? 'border-primary-500 text-primary-600'
                      : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                  } ${!tab.available ? 'opacity-50 cursor-not-allowed' : ''}`}
                  disabled={!tab.available}
                >
                  <Icon className="w-4 h-4" />
                  <span>{tab.name}</span>
                  
                  {/* Status badge for test tab */}
                  {hasStatusBadge && (
                    <span className={`ml-2 px-2 py-1 text-xs rounded-full ${
                      tab.badge === 'Ready' 
                        ? 'bg-green-100 text-green-800' 
                        : 'bg-red-100 text-red-800'
                    }`}>
                      {tab.badge}
                    </span>
                  )}
                  
                  {/* Availability indicator */}
                  {!tab.available && !isTestTab && <XCircle className="w-4 h-4 text-red-500" />}
                  
                  {/* Special indicator for test tab when feature is disabled */}
                  {isTestTab && !tab.available && (
                    <div className="flex items-center space-x-1">
                      <XCircle className="w-4 h-4 text-red-500" />
                      <span className="text-xs text-gray-400">(Disabled)</span>
                    </div>
                  )}
                </button>
              );
            })}
          </nav>
        </div>

        <div className="p-6">
          {activeTab === 'card' && renderCard()}
          {activeTab === 'config' && renderConfig()}
          {activeTab === 'examples' && renderExamples()}
          {activeTab === 'test' && renderTestModel()}
        </div>
      </div>
    </div>
  );
}; 