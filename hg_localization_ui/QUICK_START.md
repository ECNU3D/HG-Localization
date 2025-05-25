# Quick Start Guide

## 🚀 Get Started in 3 Steps

### 1. Verify Setup
```bash
python hg_localization_ui/test_setup.py
```
✅ All tests should pass

**Optional**: Test startup functionality
```bash
python hg_localization_ui/test_startup.py
```

### 2. Start the Application

**Option A: Standard startup** (logs hidden for cleaner output):
```bash
python hg_localization_ui/start_ui.py
```

**Option B: With visible logs** (see all hg_localization library logs):
```bash
python hg_localization_ui/start_with_logs.py
```

**Option C: Simple startup** (if you encounter threading issues):
```bash
python hg_localization_ui/start_simple.py
```

### 3. Open Your Browser
- **Frontend**: http://localhost:3000
- **Backend API**: http://localhost:8000/docs

## 🎯 First Time Usage

1. **Configure S3 Settings**
   - Go to the Configuration page
   - Enter your S3 bucket name
   - Choose access mode (Public or Private)
   - Save configuration

2. **Browse Datasets**
   - Navigate to Datasets page
   - View local and S3 datasets
   - Use search and filters

3. **Download a Dataset**
   - Click "Download" on any dataset
   - Monitor progress in real-time
   - View downloaded datasets locally

4. **Explore Dataset Details**
   - Click on any dataset name
   - View preview, documentation, and code examples
   - Copy usage code snippets

## 🔧 Alternative Startup Options

### Backend Only
```bash
python hg_localization_ui/start_ui.py --backend
```

### Frontend Only
```bash
python hg_localization_ui/start_ui.py --frontend
```

### Docker (Production)
```bash
cd hg_localization_ui
cp env.example .env
# Edit .env with your settings
docker-compose up -d
```

## 📚 Need Help?

- Check `README.md` for detailed documentation
- View `IMPLEMENTATION_SUMMARY.md` for technical details
- Visit http://localhost:8000/docs for API documentation

## 🎉 You're Ready!

The HG-Localization UI is now running and ready to help you manage your Hugging Face datasets with S3 integration. 