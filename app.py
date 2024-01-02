import os
import tempfile

import requests
import boto3


aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
aws_default_region = os.environ.get('AWS_DEFAULT_REGION')
endpoint_url = os.environ.get('AWS_BUCKET')


def handler(event, context):
    return download_gh_archive_hourly_data(event, context)

def download_gh_archive_hourly_data(event, context):
    date = event.get('date', event.get('queryStringParameters', {}).get('date'))
    hour = event.get('hour', event.get('queryStringParameters', {}).get('hour'))

    if not date or not hour:
        return {'statusCode': 400, 'body': 'Invalid request. Please provide date and hour.'}

    dest_bucket_name = os.environ.get('DEST_BUCKET_NAME')

    with tempfile.TemporaryDirectory() as output_dir:
        url = f"https://data.gharchive.org/{date}-{hour}.json.gz"
        file_name = f"{date}-{hour}.json.gz"
        file_path = os.path.join(output_dir, file_name)
        response = requests.get(url, timeout=60*5)

        with open(file_path, 'wb') as f:
            f.write(response.content)

        year = date.split('-')[0]
        month = date.split('-')[1]
        day = date.split('-')[2]

        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=None,
            verify=False
        )

        s3_key = f"gh-archives/year={year}/month={month}/day={day}/hour={hour}/{file_name}"
        s3_client.upload_file(file_path, dest_bucket_name, s3_key)

    return {'statusCode': 200, 'body': 'Data downloaded and uploaded to S3 successfully!'}

