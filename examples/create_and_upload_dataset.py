import sys
import os
from pathlib import Path

# Ensure the hg_localization package is in the Python path
# This is for running the example directly, adjust as needed for your project structure
SCRIPT_DIR = Path(__file__).resolve().parent
PARENT_DIR = SCRIPT_DIR.parent
sys.path.append(str(PARENT_DIR))

from datasets import Dataset, Features, Value
from hg_localization.core import upload_dataset, download_dataset, list_local_datasets, list_s3_datasets

def create_dummy_dataset():
    """Creates a simple Hugging Face Dataset for demonstration."""
    data = {
        "id": [1, 2, 3, 4, 5],
        "text": ["This is sentence one.", "Another example sentence.", "Third time is the charm.", "Fourth entry here.", "Fifth and final sentence."],
        "label": [0, 1, 0, 1, 0]
    }
    features = Features({
        "id": Value("int32"),
        "text": Value("string"),
        "label": Value("int32")
    })
    return Dataset.from_dict(data, features=features)

if __name__ == "__main__":
    print("--- Example: Creating and Uploading a Custom Dataset ---")

    # 1. Define dataset parameters
    my_dataset_id = "my_custom_text_dataset"
    my_config_name = "simple_examples"
    my_revision = "v1.0"

    # 2. Create your Hugging Face Dataset object
    print(f"\nStep 1: Creating a dummy dataset: {my_dataset_id} (config: {my_config_name}, revision: {my_revision})")
    custom_dataset = create_dummy_dataset()
    print("Dummy dataset created:")
    print(custom_dataset)

    # 3. Upload the dataset using the API
    print(f"\nStep 2: Uploading dataset '{my_dataset_id}' using the API...")
    # Ensure you have your S3 credentials (HGLOC_S3_BUCKET_NAME, etc.) in your .env file or environment
    upload_success = upload_dataset(
        dataset_obj=custom_dataset,
        dataset_id=my_dataset_id,
        config_name=my_config_name,
        revision=my_revision
    )

    if upload_success:
        print(f"Dataset '{my_dataset_id}' (config: {my_config_name}, rev: {my_revision}) processed successfully (saved locally, S3 upload attempted if configured).")
    else:
        print(f"Failed to process dataset '{my_dataset_id}'.")

    # 4. (Optional) Verify by listing and trying to load
    print("\n--- Verifying ---")
    print("\nListing local datasets after upload:")
    local_sets = list_local_datasets()
    if local_sets:
        for ds_info in local_sets:
            print(f"  - ID: {ds_info['dataset_id']}, Config: {ds_info.get('config_name', 'N/A')}, Revision: {ds_info.get('revision', 'N/A')}")
    else:
        print("  No local datasets found.")

    if os.getenv("HGLOC_S3_BUCKET_NAME"): # Only try if S3 is configured
        print("\nListing S3 datasets after upload (Note: S3 listing might take a moment to reflect changes):")
        s3_sets = list_s3_datasets()
        if s3_sets:
            for ds_info in s3_sets:
                print(f"  - ID: {ds_info['dataset_id']}, Config: {ds_info.get('config_name', 'N/A')}, Revision: {ds_info.get('revision', 'N/A')}")
        else:
            print("  No S3 datasets found or S3 not configured.")

        print(f"\nAttempting to download and load '{my_dataset_id}' (config: {my_config_name}, rev: {my_revision}) to verify integrity...")
        # You might want to clear it from local cache first if you want to force S3 download
        # import shutil
        # from hg_localization.core import _get_dataset_path
        # local_path_to_remove = _get_dataset_path(my_dataset_id, my_config_name, my_revision)
        # if local_path_to_remove.exists():
        #     print(f"  Temporarily removing {local_path_to_remove} to test S3 download...")
        #     shutil.rmtree(local_path_to_remove)

        downloaded_successfully, path = download_dataset(my_dataset_id, my_config_name, my_revision, trust_remote_code=True)
        if downloaded_successfully:
            print(f"  Successfully re-downloaded to: {path}")
            from hg_localization.core import load_local_dataset
            loaded_ds = load_local_dataset(my_dataset_id, my_config_name, my_revision)
            if loaded_ds:
                print("  Successfully loaded the dataset after re-download:")
                print(loaded_ds)
            else:
                print("  Failed to load the dataset after re-download.")
        else:
            print("  Failed to re-download the dataset.")
    else:
        print("\nS3_BUCKET_NAME not set, skipping S3 verification steps.")


    print("\n--- Conceptual CLI Upload Example ---")
    print("# If you were to build a CLI around this library, you might have a command like:")
    print(f"# python -m hg_localization.cli upload_local_dataset \\")
    print(f"# --dataset-script examples/create_and_upload_dataset.py \\") # Assuming a script that can output a dataset
    print(f"# --dataset-id \"{my_dataset_id}\" \\")
    print(f"# --config-name \"{my_config_name}\" \\")
    print(f"# --revision \"{my_revision}\"")
    print("# (This would require creating a hg_localization/cli.py with argparse and logic to call upload_dataset)")
    print("\nExample script finished.") 