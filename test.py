import boto3

# Initialize the S3 client
s3 = boto3.client('s3')

# Set the S3 bucket name and the name you want for the file in the bucket
bucket_name = 'steam-processing-madhu'
file_name = 'example.csv'  # Change this to your desired file name

# Specify the local file path of the CSV file you want to upload
local_file_path = '/home/ec2-user/example.csv'

# Specify the S3 key (path in the bucket) where the file will be stored
s3_key = file_name

# Upload the file to S3
s3.upload_file(local_file_path, bucket_name, s3_key)

print(f'File {file_name} uploaded to S3 bucket {bucket_name} with key {s3_key}')
