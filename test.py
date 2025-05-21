# import boto3
# import os
# from botocore.config import Config

# endpoint = 'http://oss-cn-beijing.aliyuncs.com'
# # 通过环境变量传递临时访问凭证信息。

# # session_token = os.getenv('OSS_SessionToken')

# s3 = boto3.client(
#     's3',
#     aws_access_key_id=access_key_id,
#     aws_secret_access_key=secret_access_key,
#     # aws_session_token=session_token,
#     endpoint_url=endpoint,
#     config=Config(s3={"addressing_style": "virtual", "aws_chunked_encoding_enabled": False},
#                       signature_version='v4')
#     )
# print(s3)
# # upload file to a specific bucket
# bucket_name = 'zheyu-huggingface-test'
# file_name = 'test.txt'

# s3.upload_file(
#     file_name,
#     bucket_name,
#     file_name
# )

# # set the file to public
# s3.put_object_acl(
#     Bucket=bucket_name,
#     Key=file_name,
#     ACL='public-read'
# )

# # get the public url
# public_url = f"https://{bucket_name}.oss-cn-beijing.aliyuncs.com/{file_name}"
# print(public_url)


from hg_localization import load_local_dataset, DATASETS_STORE_PATH
import os

print("Attempting to load datasets from local cache (or S3 if not found locally)...")
print(f"Local cache directory: {DATASETS_STORE_PATH}\n")

# Example 1: Load 'glue/mrpc' (assuming it was downloaded by download_example.py)
print("Attempting to load glue/mrpc...")
# Ensure this matches a dataset/config/revision you expect to be in your cache or S3
mrpc_dataset = load_local_dataset(
    dataset_id="nyu-mll/glue",
    config_name="mrpc",
    revision=None  # Or specify the exact revision if it was downloaded with one
)
