from hg_localization import download_dataset

# Example 1: Download 'glue' dataset, 'mrpc' config, default revision
print("Attempting to download glue/mrpc...")
success, message = download_dataset(
    dataset_id="glue", 
    config_name="mrpc", 
    revision=None,
    trust_remote_code=True
)
if success:
    print(f"Successfully processed dataset. Local path: {message}")
else:
    print(f"Failed to process dataset. Reason: {message}")

print("\n" + "="*50 + "\n")

# Example 2: Download 'wikiann' dataset, 'en' config, specific revision (e.g., 'main')
# Note: Replace 'main' with an actual valid revision for wikiann/en if needed.
print("Attempting to download wikiann/en (revision: main)...")
success, message = download_dataset(
    dataset_id="wikiann",
    config_name="en",
    revision="main",  # Replace with a specific tag or commit hash if desired
    trust_remote_code=False # Usually not needed for standard datasets
)
if success:
    print(f"Successfully processed dataset. Local path: {message}")
else:
    print(f"Failed to process dataset. Reason: {message}")

print("\n" + "="*50 + "\n")

# Example 3: Download a dataset with default config and revision
# Replace 'your_username/your_dataset' with an actual small dataset for testing
# For instance, using 'hf-internal-testing/dummy_dataset' which is small.
print("Attempting to download hf-internal-testing/dummy_dataset (default config/revision)...")
success, message = download_dataset(
    dataset_id="hf-internal-testing/dummy_dataset",
    config_name=None, # Defaults to the first config or 'default_config'
    revision=None,    # Defaults to 'main' or 'default_revision'
    trust_remote_code=False
)
if success:
    print(f"Successfully processed dataset. Local path: {message}")
else:
    print(f"Failed to process dataset. Reason: {message}")

print("\n" + "="*50 + "\n")
print("Download examples finished.") 