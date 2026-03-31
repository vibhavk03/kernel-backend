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

    def _build_key(self, key: str) -> str:
        # if base_prefix is set, prepend it to the key if it's not already there
        if self.base_prefix and not key.startswith(self.base_prefix):
            return f"{self.base_prefix}{key}"
        return key

    def get_file_bytes(self, key: str) -> BytesIO:
        # get only file from s3 without metadata
        full_key = self._build_key(key)
        try:
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=full_key,
            )
            file_bytes = response["Body"].read()
            return BytesIO(file_bytes)
        except ClientError as e:
            raise FileNotFoundError(f"Could not fetch S3 object: {full_key}") from e

    def get_file_metadata(self, key: str) -> dict:
        # get only metadata from s3 without file bytes
        full_key = self._build_key(key)
        try:
            response = self.client.head_object(
                Bucket=self.bucket_name,
                Key=full_key,
            )
            return {
                "bucket": self.bucket_name,
                "key": full_key,
                "etag": response.get("ETag", "").replace('"', ""),
                "last_modified": response.get("LastModified"),
                "size": response.get("ContentLength"),
                "content_type": response.get("ContentType"),
            }
        except ClientError as e:
            raise FileNotFoundError(
                f"Could not fetch metadata for S3 object: {full_key}"
            ) from e

    def get_file_with_metadata(self, key: str) -> tuple[BytesIO, dict]:
        # get both file bytes and metadata from s3
        full_key = self._build_key(key)
        try:
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=full_key,
            )
            file_bytes = response["Body"].read()

            metadata = {
                "bucket": self.bucket_name,
                "key": full_key,
                "etag": response.get("ETag", "").replace('"', ""),
                "last_modified": response.get("LastModified"),
                "size": response.get("ContentLength"),
                "content_type": response.get("ContentType"),
            }
            print(f"Fetched file:{full_key} from S3: {metadata}")

            return BytesIO(file_bytes), metadata
        except ClientError as e:
            raise FileNotFoundError(f"Could not fetch S3 object: {full_key}") from e
