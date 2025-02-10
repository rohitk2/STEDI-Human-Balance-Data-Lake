import os
import boto3
import configparser
from botocore.exceptions import ClientError

# Read AWS credentials from configuration
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

# Define the S3 bucket name
S3_BUCKET_NAME = "udacity-data-lake-project-rohit1998"

# Initialize the S3 client
s3_client = boto3.client('s3', region_name="us-west-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET)

def create_bucket(bucket_name, client):
    """
    Create an S3 bucket if it doesn't already exist.
    """
    try:
        print(f"Creating bucket: {bucket_name}")
        client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
        )
        print(f"Bucket '{bucket_name}' created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print(f"Bucket '{bucket_name}' already exists.")
        else:
            print(f"Error creating bucket: {e}")

def upload_directory_to_s3(local_dir, bucket_name, client):
    """
    Upload the contents of a local directory to S3, preserving the directory structure.
    """
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            # Construct the full local file path
            local_file_path = os.path.join(root, file)
            
            # Construct the S3 object key (preserving folder structure)
            relative_path = os.path.relpath(local_file_path, local_dir)
            s3_object_key = os.path.join(relative_path)
            
            # Upload the file to S3
            try:
                print(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_object_key}...")
                client.upload_file(local_file_path, bucket_name, s3_object_key)
                print(f"Uploaded: {s3_object_key}")
            except ClientError as e:
                print(f"Error uploading {local_file_path}: {e}")


# Create the S3 bucket
create_bucket(S3_BUCKET_NAME, s3_client)
    
# Local directory to upload
local_directory = "S3_Data"
    
# Upload the directory to S3
upload_directory_to_s3(local_directory, S3_BUCKET_NAME, s3_client)
print("All files uploaded successfully.")