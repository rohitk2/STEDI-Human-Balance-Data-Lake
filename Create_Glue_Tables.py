import os
import boto3
import configparser
from botocore.exceptions import ClientError

# Read AWS credentials from configuration
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
NUM_TABLES = 3

# Initialize the AWS Glue client
glue_client = boto3.client('glue', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)  # Change to your region

# Define the S3 bucket and paths for each landing and trusted zone
S3_BUCKET = "udacity-data-lake-project-rohit1998"
landing_zones = {
    "accelerometer_landing": f"s3://{S3_BUCKET}/accelerometer_landing/",
    "customer_landing": f"s3://{S3_BUCKET}/customer_landing/",
    "step_trainer_landing": f"s3://{S3_BUCKET}/step_trainer_landing/",
}

trusted_zones = {
    "customer_trusted": f"s3://{S3_BUCKET}/customer_trusted/",
    "accelerometer_trusted": f"s3://{S3_BUCKET}/accelerometer_trusted/",
    "step_trainer_trusted": f"s3://{S3_BUCKET}/step_trainer_trusted/",
    "machine_learning_curated": f"s3://{S3_BUCKET}/machine_learning_curated/",
    "customer_curated": f"s3://{S3_BUCKET}/customer_curated/",  # New curated table
}

# Schemas for landing zones
customer_schema = [
    {"Name": "serialnumber", "Type": "string"},
    {"Name": "sharewithpublicasofdate", "Type": "string"},
    {"Name": "birthday", "Type": "string"},
    {"Name": "registrationdate", "Type": "string"},
    {"Name": "sharewithresearchasofdate", "Type": "string"},
    {"Name": "customername", "Type": "string"},
    {"Name": "email", "Type": "string"},
    {"Name": "lastupdatedate", "Type": "string"},
    {"Name": "phone", "Type": "string"},
    {"Name": "sharewithfriendsasofdate", "Type": "string"},
]

step_trainer_schema = [
    {"Name": "sensorReadingTime", "Type": "timestamp"},
    {"Name": "serialNumber", "Type": "string"},
    {"Name": "distanceFromObject", "Type": "double"},
]

accelerometer_schema = [
    {"Name": "timeStamp", "Type": "timestamp"},
    {"Name": "user", "Type": "string"},
    {"Name": "x", "Type": "double"},
    {"Name": "y", "Type": "double"},
    {"Name": "z", "Type": "double"},
]

# Schema for machine_learning_curated
ML_Curated_schema = [
    {"Name": "user", "Type": "string"},
    {"Name": "sensorReadingTime", "Type": "timestamp"},
    {"Name": "serialNumber", "Type": "string"},
    {"Name": "distanceFromObject", "Type": "double"},
    {"Name": "x", "Type": "double"},
    {"Name": "y", "Type": "double"},
    {"Name": "z", "Type": "double"},
]

# Schemas for trusted zones (same as corresponding landing zones)
# Update trusted_schemas to include customer_curated and machine_learning_curated
trusted_schemas = {
    "customer_trusted": customer_schema,
    "customer_curated": customer_schema,  # Schema for the curated customer table
    "accelerometer_trusted": accelerometer_schema,
    "step_trainer_trusted": step_trainer_schema,
    "machine_learning_curated": ML_Curated_schema,  # Explicit schema for machine_learning_curated
}

# Define the Glue database name
GLUE_DATABASE_NAME = "data_lake_project"

# Create Glue database if it doesn't exist
def create_glue_database(database_name):
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": database_name,
                "Description": "Database for the landing zones in the data lake"
            }
        )
        print(f"Glue database '{database_name}' created successfully.")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Glue database '{database_name}' already exists.")

# Create Glue table
def create_glue_table(database_name, table_name, s3_path, schema):
    try:
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput={
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": schema,
                    "Location": s3_path,
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                        "Parameters": {"paths": "attribute1,attribute2,attribute3"}
                    }
                },
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    "classification": "json",
                    "compressionType": "none",
                }
            }
        )
        print(f"Glue table '{table_name}' created successfully.")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Glue table '{table_name}' already exists.")
    except Exception as e:
        print(f"Error creating table '{table_name}': {e}")

# Create Glue table without a schema
def create_glue_table_no_schema(database_name, table_name, s3_path):
    try:
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput={
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": [],  # No schema defined
                    "Location": s3_path,
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                    }
                },
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    "classification": "json",
                    "compressionType": "none"
                }
            }
        )
        print(f"Glue table '{table_name}' created successfully.")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Glue table '{table_name}' already exists.")
    except Exception as e:
        print(f"Error creating table '{table_name}': {e}")


def main():
    # Create Glue database
    create_glue_database(GLUE_DATABASE_NAME)

    # Create landing zone tables
    for table_name, s3_path in landing_zones.items():
        schema = trusted_schemas.get(table_name.replace("_landing", "_trusted"), [])
        create_glue_table(GLUE_DATABASE_NAME, table_name, s3_path, schema)

    # Create trusted zone tables, including customer_curated and machine_learning_curated
    for table_name, s3_path in trusted_zones.items():
        schema = trusted_schemas.get(table_name, [])
        if schema:
            create_glue_table(GLUE_DATABASE_NAME, table_name, s3_path, schema)
        else:
            create_glue_table_no_schema(GLUE_DATABASE_NAME, table_name, s3_path)


if __name__ == "__main__":
    main()
