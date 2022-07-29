"""
Constants are stored in this file
"""

from enum import Enum

class S3FileTypes(Enum):
    CSV = 'csv'
    PARQUET = 'parquet'

class MetaProcessFormat(Enum):
    """
    For MetaProcess class
    """

    META_DATE_FORMAT = '%Y-%m-%d'
    META_PROCESS_DATE_FORMAT = '%Y%m%d %H%M%S'
    META_DATE_COL = 'source_date'
    META_PROCESSED_DATE_COL = 'datetime_of_processing'
    META_FILE_FORMAT = 'csv'
