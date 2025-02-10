import boto3
import configparser
from botocore.exceptions import ClientError

def create_s3_bucket(bucket_name, region, s3_client):
    try:
        response = s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': region}
        )
        print(f"Bucket {bucket_name} created successfully.")
    except ClientError as e:
        print(f"Error creating bucket: {e}")
        return None
    return response

def update_athena_workgroup(bucket_name, workgroup_name, athena_client):
    output_location = f's3://{bucket_name}/'
    try:
        response = athena_client.update_work_group(
            WorkGroup=workgroup_name,
            ConfigurationUpdates={
                'ResultConfigurationUpdates': {
                    'OutputLocation': output_location
                }
            }
        )
        print(f"Athena workgroup {workgroup_name} configured to use {output_location} for query results.")
    except ClientError as e:
        print(f"Error updating Athena workgroup: {e}")

def main():
    # Load AWS credentials from configuration file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    REGION = 'us-west-2'  # Replace with your desired AWS region

    # Initialize clients
    s3 = boto3.client(
        's3',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name=REGION
    )

    athena = boto3.client(
        'athena',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name=REGION
    )

    # Define S3 bucket and Athena workgroup names
    bucket_name = 'my-athena-query-results-rohit1998'
    workgroup_name = 'primary'  # Replace with your custom workgroup name if needed

    # Create S3 bucket
    create_s3_bucket(bucket_name, REGION, s3)

    # Update Athena workgroup
    update_athena_workgroup(bucket_name, workgroup_name, athena)

if __name__ == '__main__':
    main()
