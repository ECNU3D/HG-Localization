import os
from hg_localization import list_s3_datasets

print("Listing datasets from S3 bucket...\n")

# Check if S3 bucket is configured
s3_bucket_name = os.getenv("HGLOC_S3_BUCKET_NAME")
if not s3_bucket_name:
    print("S3_BUCKET_NAME environment variable is not set.")
    print("S3 operations will be skipped. Please configure S3 to run this example fully.")
else:
    print(f"Target S3 Bucket: {s3_bucket_name}\n")
    s3_sets = list_s3_datasets()
    if not s3_sets:
        print("No datasets found in the S3 bucket or S3 is not configured.")
        print("Ensure datasets have been uploaded (e.g., by running download_example.py with S3 configured).")
    else:
        print(f"Found {len(s3_sets)} dataset entries in S3 bucket '{s3_bucket_name}':")
        for ds_info in s3_sets:
            print(
                f"  ID: {ds_info.get('dataset_id')}, "
                f"Config: {ds_info.get('config_name')}, "
                f"Revision: {ds_info.get('revision')}"
            )

print("\nS3 listing example finished.") 