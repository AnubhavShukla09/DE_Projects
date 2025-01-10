import boto3
from datetime import datetime


def save_to_s3():
    s3 = boto3.client(
        "s3",
        aws_access_key_id="____YOUR_ACCESS_KEY____",
        aws_secret_access_key="____YOUR_SECRET_KEY____",
    )

    bucket_name = "your-s3-bucket"
    filename = f"reddit_sentiment_{datetime.now().strftime('%Y-%m-%d')}.csv"

    s3.upload_file("/tmp/reddit_posts_with_sentiment.csv", bucket_name, filename)
