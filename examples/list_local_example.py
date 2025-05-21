from hg_localization import list_local_datasets, DATASETS_STORE_PATH

print(f"Listing datasets from local cache: {DATASETS_STORE_PATH}\n")

local_sets = list_local_datasets()

if not local_sets:
    print("No datasets found in the local cache.")
    print(f"Please run the download_example.py script first or manually place datasets in {DATASETS_STORE_PATH}")
else:
    print(f"Found {len(local_sets)} dataset entries locally:")
    for ds_info in local_sets:
        print(
            f"  ID: {ds_info.get('dataset_id')}, "
            f"Config: {ds_info.get('config_name')}, "
            f"Revision: {ds_info.get('revision')}, "
            f"Path: {ds_info.get('path')}"
        )

print("\nLocal listing example finished.") 