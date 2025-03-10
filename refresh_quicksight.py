import boto3
from datetime import datetime

# Initialize the QuickSight client
client = boto3.client('quicksight')
dataset_map = {'glue_job_name': 'quicksight_dataset_id'}
aws_account_id = 'my_aws_account'

def lambda_handler(event, context):
    print('event:', event, type(event))
    glue_job_name = event['detail']['jobName']
    print('glue_job_name:', glue_job_name)
    dataset_id = dataset_map[glue_job_name]

    dt_string = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    inguestion_id = dt_string + '_' + dataset_id

    # Trigger the dataset refresh
    try:
        response = client.create_ingestion(
            DataSetId=dataset_id,
            IngestionId=inguestion_id,  # You can generate a UUID for this
            AwsAccountId=aws_account_id
        )
        print(f"Refresh triggered successfully: {inguestion_id}")
    except Exception as e:
        print(f"Error triggering refresh: {e}")
