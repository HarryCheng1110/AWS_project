import json
import boto3
import re
import copy
import urllib.parse
 
glue = boto3.client('glue')
database_name = 'my_databse'

def lambda_handler(event, context):
    # Extract bucket name and object key from the S3 event
    for record in event['Records']:
        print('record:', record)
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']
        object_key = urllib.parse.unquote(object_key)
        print('object_key', object_key)

        object_key_split = object_key.split('/')

        my_prefix = object_key_split[2]
        my_table = object_key_split[3]
        year = object_key_split[-4].split('=')[1]
        month = object_key_split[-3].split('=')[1]
        day = object_key_split[-2].split('=')[1]

        table_name = f'raw_{my_prefix}_{my_table}'
        partition_location = f"s3://{bucket_name}/{object_key.rsplit('/', 1)[0]}/"

        # Add partition to the Glue Data Catalog without specifying columns
        try:
            print('Retrieve Table Details')
            get_table_response = glue.get_table(
                DatabaseName=database_name,
                Name=table_name
            )
            
            print('partition_location:', partition_location)
            storage_descriptor = get_table_response['Table']['StorageDescriptor']
            custom_storage_descriptor = copy.deepcopy(storage_descriptor)
            custom_storage_descriptor['Location'] = partition_location
            print('custom_storage_descriptor:', custom_storage_descriptor)

            
            response = glue.create_partition(
                DatabaseName=database_name,  # Replace with your Glue database name
                TableName=table_name,        # Replace with your Glue table name
                PartitionInput={
                    'Values': [year, month, day],  # Partition values
                    'StorageDescriptor': custom_storage_descriptor
                }
            )
            print(f"Partition added successfully: {response}")
        except glue.exceptions.AlreadyExistsException:
            print(f"Partition already exists for {year}, {month}, {day}")
        except Exception as e:
            print(f"Error adding partition: {str(e)}")
    else:
        print(f"Object {object_key} does not match the expected pattern.")

