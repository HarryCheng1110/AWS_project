import io
import json
import boto3
import urllib.parse
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.json as pj

s3 = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            print('record:', record)
            bucket_name = record['s3']['bucket']['name']
            object_key = record['s3']['object']['key']
            object_key = urllib.parse.unquote(object_key)
            print('object_key', object_key)

            object_key_split = object_key.split('/')

            my_prefix = object_key_split[2]
            year = object_key_split[-4].split('=')[1]
            month = object_key_split[-3].split('=')[1]
            day = object_key_split[-2].split('=')[1]
            timestamp = object_key_split[-1].split('.json')[0]

            json_file = s3.get_object(
                Bucket=bucket_name,
                Key=object_key,
            )['Body'].read()

            # Use BytesIO to create a file-like object for pyarrow
            json_buffer = io.BytesIO(json_file)

            pa_table = pj.read_json(json_buffer)

            file_name = f"{timestamp}.snappy.parquet"
            file_path=f"/tmp/" + file_name
            s3_loc = f"my_bucket/my_prefix={my_prefix}/year={year}/month={month}/day={day}/"

            pq.write_table(pa_table, file_path)

            s3.upload_file(
                Filename=file_path,
                Bucket=bucket_name,
                Key=s3_loc + file_name
            )

            status_code, body = 200, f'Sucessfully turn json into parquet: {s3_loc + file_name}'
        except Exception as e:
            status_code, body = 500, f"Error occurs: {str(e)}"

    return {
        'statusCode': status_code,
        'body': json.dumps(body)
    }
