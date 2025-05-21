from hg_localization import load_local_dataset, DATASETS_STORE_PATH
import os

print("Attempting to load datasets from local cache (or S3 if not found locally)...")
print(f"Local cache directory: {DATASETS_STORE_PATH}\n")

# Example 1: Load 'glue/mrpc' (assuming it was downloaded by download_example.py)
print("Attempting to load glue/mrpc...")
# Ensure this matches a dataset/config/revision you expect to be in your cache or S3
mrpc_dataset = load_local_dataset(
    dataset_id="glue",
    config_name="mrpc",
    revision=None  # Or specify the exact revision if it was downloaded with one
)

if mrpc_dataset:
    print("Successfully loaded glue/mrpc dataset!")
    # You can inspect the dataset, e.g., print a sample:
    # print("Sample from train split:", mrpc_dataset["train"][0])
    # print("Dataset features:", mrpc_dataset.features)
    # print("Available splits:", list(mrpc_dataset.keys()))
else:
    print("Failed to load glue/mrpc dataset.")
    print("Please ensure it exists in local cache or S3 (and S3 is configured if applicable).")
    print(f"Checked local path: {DATASETS_STORE_PATH}/glue/mrpc/default_revision")

print("\n" + "="*50 + "\n")

# Example 2: Load 'hf-internal-testing/dummy_dataset' (default config/revision)
print("Attempting to load hf-internal-testing/dummy_dataset...")
dummy_dataset = load_local_dataset(
    dataset_id="hf-internal-testing/dummy_dataset",
    config_name=None, # Will try to load default_config
    revision=None     # Will try to load default_revision
)

if dummy_dataset:
    print("Successfully loaded hf-internal-testing/dummy_dataset!")
    # print("Sample from train split:", dummy_dataset["train"][0])
else:
    print("Failed to load hf-internal-testing/dummy_dataset.")
    print(f"Checked local path: {DATASETS_STORE_PATH}/hf-internal-testing_dummy_dataset/default_config/default_revision")

print("\n" + "="*50 + "\n")

# Note on S3 fallback:
# If HGLOC_S3_BUCKET_NAME is set and the dataset is not found locally,
# load_local_dataset will attempt to fetch it from S3.

print("Load local examples finished.")
print("To test S3 fallback: configure S3, run download_example.py, then remove a dataset from local cache ")
print(f"(e.g., delete the folder for 'glue/mrpc' under {DATASETS_STORE_PATH}) and re-run this script.") 