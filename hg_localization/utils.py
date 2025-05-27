from pathlib import Path
from typing import Optional
import zipfile
import os

# Configuration (Imported from a central config or defined directly if utils is standalone)
# from .config import DATASETS_STORE_PATH, DEFAULT_CONFIG_NAME, DEFAULT_REVISION_NAME, S3_DATA_PREFIX
# For now, let's assume these might be passed or globally accessible if not using direct .config import here
# This part might need adjustment based on how config is structured relative to utils.py

def _get_safe_path_component(name: Optional[str]) -> str:
    """Replaces characters unsafe for file/path names with underscores."""
    if not name:
        return ""
    
    # Replace unsafe characters with underscores, but preserve single quotes
    return name.replace("/", "_").replace("\\", "_").replace(":", "_").replace("*", "_").replace("?", "_").replace("\"", "_").replace("<", "_").replace(">", "_").replace("|", "_").replace(" ", "_")

def _restore_dataset_name(safe_name: Optional[str]) -> str:
    """Converts a safe path component back to original dataset name format.
    
    This function attempts to restore the original dataset name by converting
    underscores back to forward slashes. It uses a heuristic approach:
    - If there's only one underscore, convert it to a forward slash
    - If there are multiple underscores, convert the last one to a forward slash
      (assuming the most common pattern is org/dataset_name)
    
    Examples:
        'dreamerdeo_finqa' -> 'dreamerdeo/finqa'
        'my_data_set_realdataset' -> 'my_data_set/realdataset'
        'simple_dataset' -> 'simple/dataset'
        'huggingface_datasets' -> 'huggingface/datasets'
        'microsoft_DialoGPT_medium' -> 'microsoft/DialoGPT_medium'
    """
    if not safe_name:
        return ""
    
    # Find the last underscore and replace it with a forward slash
    last_underscore_index = safe_name.rfind('_')
    if last_underscore_index == -1:
        # No underscore found, return as is
        return safe_name
    
    # Replace only the last underscore with a forward slash
    return safe_name[:last_underscore_index] + '/' + safe_name[last_underscore_index + 1:]

def _zip_directory(directory_path: Path, zip_path: Path) -> bool:
    """Zips the contents of a directory."""
    if not directory_path.is_dir():
        print(f"Error: {directory_path} is not a valid directory to zip.")
        return False
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for item in directory_path.rglob('*'):
                arcname = item.relative_to(directory_path)
                zipf.write(item, arcname=arcname)
        print(f"Successfully zipped {directory_path} to {zip_path}")
        return True
    except Exception as e:
        print(f"Error zipping directory {directory_path}: {e}")
        return False

def _unzip_file(zip_path: Path, extract_to_path: Path) -> bool:
    """Unzips a file to a specified directory."""
    if not zip_path.is_file():
        print(f"Error: {zip_path} is not a valid zip file.")
        return False
    os.makedirs(extract_to_path, exist_ok=True)
    try:
        with zipfile.ZipFile(zip_path, 'r') as zipf:
            zipf.extractall(extract_to_path)
        print(f"Successfully unzipped {zip_path} to {extract_to_path}")
        return True
    except Exception as e:
        print(f"Error unzipping file {zip_path}: {e}")
        return False
