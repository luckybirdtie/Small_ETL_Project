""""
Docstrings. S3 Connector and Methods.
"""
# The OS module in Python provides functions for interacting with the operating system. OS comes under Python’s standard utility # modules. The *os* and *os.path* modules include many functions to interact with the file system.
import os
import logging
# Boto3 is the Amazon Web Services (AWS) Software Development Kit (SDK) for Python
import boto3


class S3BucketConnector():
    """
    Class for interacting with S3 Buckets.
    """

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str):
        """
        Constructor.

        :param access_key: S3 access key.
        :param secret_key: S3 access secret key.
        :param endpoint_url: S3 endpoint.
        :param bucket: S3 bucket name.
        """
        self._logger = logging.getLogger(__name__)

        self.endpoint_url = endpoint_url
        #The access_key is the env name not the key itself. os.environ extract the real keys
        self.session = boto3.Session(aws_access_key_id = os.environ[access_key],
                                     aws_secret_access_key = os.environ[secret_key])
        #python convention - '__' before a name indicate it is private, '_' denote protected
        self._s3 = self.session.resource(service_name = 's3', endpoint_url = endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self, prefix: str):
        """
        List all keys with the :param prefix
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix = prefix)]
        return files

    def read_csv_to_df(self):
        pass

    def write_df_to_s3(self):
        pass
