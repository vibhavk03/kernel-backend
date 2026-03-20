import os
from io import BytesIO
import boto3
from botocore.exceptions import ClientError

from app.core.config import settings


class S3Service:
    def __init__(self):
        self.bucket_name = settings.s3_bucket_name
        self.base_prefix = settings.s3_base_prefix or ""
        self.client = boto3.client(
            "s3",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
        )

    def get_file_bytes(self, key: str) -> BytesIO:
        try:
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=key,
            )
            file_bytes = response["Body"].read()
            return BytesIO(file_bytes)
        except ClientError as e:
            raise FileNotFoundError(f"Could not fetch S3 object: {key}") from e
