import boto3
import datetime
import dotenv  
import os

dotenv.load_dotenv()

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_session_token = os.getenv('AWS_SESSION_TOKEN')
bucket_name = os.getenv('S3_BUCKET_NAME')

def delete_todays_data():
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
    today_date = datetime.date.today().strftime('%Y-%m-%d')
    objects = s3.list_objects(Bucket=bucket_name)

    counter = 0
    for obj in objects.get('Contents', []):
        if obj['Key'].startswith(today_date + '-file'):
            print(f"Deleting: {obj['Key']}")
            s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
            counter += 1

    print("Deleted {} Files.".format(counter))
