import os
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Load .env
load_dotenv()
bucket = os.getenv("S3_BUCKET_NAME")
region = os.getenv("AWS_REGION")

# Initialize client
s3 = boto3.client("s3", region_name=region)

try:
    # Try listing first 1 item to verify connection
    response = s3.list_objects_v2(Bucket=bucket, MaxKeys=1)

    print(f"✅ Successfully connected to bucket: {bucket}")
    if "Contents" in response:
        print(f"📦 Bucket is not empty. Found at least 1 object.")
    else:
        print(f"📭 Bucket is currently empty.")
except ClientError as e:
    print(f"❌ Failed to access bucket: {e}")
