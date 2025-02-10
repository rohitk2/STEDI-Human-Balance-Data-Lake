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

S3_BUCKET_NAME = "udacity-data-lake-project-rohit1998"

def delete_bucket_and_contents(bucket_name):
    try:
        # List and delete all objects in the bucket
        print(f"Deleting all objects in bucket: {bucket_name}...")
        objects = s3_client.list_objects_v2(Bucket=bucket_name)

        if 'Contents' in objects:
            for obj in objects['Contents']:
                print(f"Deleting object: {obj['Key']}...")
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])

        # Check for additional object versions (if bucket versioning is enabled)
        response = s3_client.list_object_versions(Bucket=bucket_name)
        if 'Versions' in response:
            for version in response['Versions']:
                print(f"Deleting object version: {version['Key']} (VersionId: {version['VersionId']})...")
                s3_client.delete_object(
                    Bucket=bucket_name,
                    Key=version['Key'],
                    VersionId=version['VersionId']
                )

        if 'DeleteMarkers' in response:
            for marker in response['DeleteMarkers']:
                print(f"Deleting delete marker: {marker['Key']} (VersionId: {marker['VersionId']})...")
                s3_client.delete_object(
                    Bucket=bucket_name,
                    Key=marker['Key'],
                    VersionId=marker['VersionId']
                )

        # Delete the bucket itself
        print(f"Deleting bucket: {bucket_name}...")
        s3_client.delete_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' deleted successfully.")
    except ClientError as e:
        print(f"Error deleting bucket '{bucket_name}': {e}")

# Loop through and delete each bucket
delete_bucket_and_contents(S3_BUCKET_NAME)