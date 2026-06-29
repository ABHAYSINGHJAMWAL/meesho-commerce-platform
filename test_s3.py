import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    's3',
    endpoint_url=os.getenv('S3_ENDPOINT'),
    aws_access_key_id=os.getenv('S3_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('S3_SECRET_KEY'),
    region_name=os.getenv('AWS_REGION', 'us-east-1')
)

bucket = os.getenv('S3_BUCKET')

# Write
s3.put_object(
    Bucket=bucket,
    Key='bronze/test/hello.txt',
    Body=b'Meesho data lake working'
)
print(f"Written to s3://{bucket}/bronze/test/hello.txt")

# Read back
response = s3.get_object(Bucket=bucket, Key='bronze/test/hello.txt')
print(f"Read back: {response['Body'].read().decode()}")

print("\nMinIO S3 working perfectly")