""""
Docstrings. S3 Connector and Methods.
"""
# The OS module in Python provides functions for interacting with the operating system.
# OS comes under Python’s standard utility # modules.
# The *os* and *os.path* modules include many functions to interact with the file system.
import os
import logging
from io import BytesIO, StringIO

# Boto3 is the Amazon Web Services (AWS) Software Development Kit (SDK) for Python
import boto3
import pandas as pd

from src.common.constants import S3FileTypes
from src.common.custom_exceptions import WrongFormatException


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
        Return all keys of files that match with the :param prefix.
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix = prefix)]
        return files

    def read_csv_to_df(self, key: str, encoding: str = 'utf-8', sep: str = ','):
        """
        Read data from a csv file retrieved from S3 bucket and return a dataframe.

        :param key: key of the file to be retrived
        :param encoding: encoding of the data in the csv file
        :param sep: delimiter in the csv file

        returns:
            data_frame: pandas Dataframe generated from the data in the csv file
        """

        self._logger.info('Read file %s%s%s', self.endpoint_url, self._bucket.name, key)
        csv_obj = self._bucket.Object(key = key).get().get('Body').read().decode(encoding)
        data = StringIO(csv_obj)
        data_frame = pd.read_csv(data, sep=sep)
        return data_frame

    def write_df_to_s3(self, data_frame: pd.DataFrame, key: str, file_format: str):
        """
        Write a Pandas DataFrame to S3.
        Supported formats: .csv, .parquet.

        :param data_frame: source data frame to be sent to S3.
        :param key: key of the target file.
        :param file_format: format of the target file.
        """

        if data_frame.empty:
            self._logger.info('The dataframe is empty! No file will be written!')
            return None
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        self._logger.info('The file format %s is not '
        'supported to be written to s3!', file_format)
        raise WrongFormatException

    def __put_object(self, out_buffer: StringIO or BytesIO, key: str):
        """
        Helper function for self.write_df_to_s3()

        :out_buffer: StringIO | BytesIO that should be written
        :key: target key of the saved file
        """
        self._logger.info('Writing file to %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True
