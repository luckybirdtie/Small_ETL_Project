"""
Custom Exceptions
"""

class WrongFormatException(Exception):
    """
    Exception that can be raised when the format type
    given as a parameter is not supported.
    """

class WrongMetaFileException(Exception):
    """
    Exception that can be raised when the existing
    meta file format is not consistent with the new 
    meta file format.
    """