import boto3
from boto3.s3.transfer import TransferConfig

from ..log import GlueLogger as logger


class S3Utils:
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, region_name=None):
        """
        Initialize the S3Utils class with optional AWS credentials and region.
        If no credentials are provided, boto3 will use the default credentials chain.
        
        :param aws_access_key_id: AWS access key ID
        :param aws_secret_access_key: AWS secret access key
        :param region_name: AWS region name
        """
        self.s3_client = boto3.client('s3',
                                      aws_access_key_id=aws_access_key_id,
                                      aws_secret_access_key=aws_secret_access_key,
                                      region_name=region_name)


    def create_bucket(self, bucket_name, region=None):
        """
        Create a new S3 bucket in the specified region.
        
        :param bucket_name: Name of the new S3 bucket
        :param region: AWS region to create the bucket in
        """
        if region is None:
            self.s3_client.create_bucket(Bucket=bucket_name)
        else:
            location = {'LocationConstraint': region}
            self.s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        logger.info(f"Bucket {bucket_name} created.")

    def list_buckets(self):
        """
        List all S3 buckets in the account.
        
        :return: List of bucket names
        """
        response = self.s3_client.list_buckets()
        return [bucket['Name'] for bucket in response['Buckets']]
    
    def list_objects(self, bucket_name, prefix=''):
        """
        List all objects in the specified bucket and prefix.
        
        :param bucket_name: Name of the S3 bucket
        :param prefix: Prefix to filter objects
        :return: List of object keys
        """
        paginator = self.s3_client.get_paginator('list_objects_v2')
        result = []
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                result.extend([obj['Key'] for obj in page['Contents']])
        return result

    def copy_objects(self, source_bucket, source_key, dest_bucket, dest_key):
        """
        Copy an object from one bucket to another using fast transfer (multipart upload).
        
        :param source_bucket: Source bucket name
        :param source_key: Source object key
        :param dest_bucket: Destination bucket name
        :param dest_key: Destination object key
        """
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                                multipart_chunksize=1024 * 25, use_threads=True)
        self.s3_client.copy(copy_source, dest_bucket, dest_key, Config=config)
        logger.info(f"Copied {len(source_key)} files from {source_bucket} to {dest_bucket}")

    def delete_objects(self, bucket_name, prefix=''):
        """
        Delete all objects in the specified bucket and prefix (recursive deletion).
        
        :param bucket_name: Name of the S3 bucket
        :param prefix: Prefix to filter objects for deletion
        """
        # List objects to delete
        objects_to_delete = self.list_objects(bucket_name, prefix)
        if not objects_to_delete:
            logger.info("No objects to delete.")
            return

        # Prepare a list of objects to delete
        delete_objects = [{'Key': key} for key in objects_to_delete]
        # Delete objects in the bucket
        response = self.s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_objects})
        logger.info(f"Deleted objects: {response.get('Deleted', [])}")

    def move_objects(self, bucket, prefix = '') -> None:
        """
        Move all objects from the specified bucket and prefix to .
        
        :param bucket_name: Name of the S3 bucket
        :param prefix: Prefix to filter objects for deletion
        """
        self.copy_object(bucket, prefix)
        self.delete_objects(bucket, prefix)