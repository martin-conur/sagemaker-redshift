"""
redshift-utils: A Python utility library for efficient data operations between Amazon Redshift and S3
"""

__version__ = "0.2.0"
__author__ = "Martin Contreras"
__email__ = "martincontrerasur@gmail.com"

from .redshift_utils import (
    unload_redshift,
    copy_to_redshift,
    copy_s3_to_redshift,
    verify_s3_files
)

__all__ = [
    "unload_redshift",
    "copy_to_redshift",
    "copy_s3_to_redshift",
    "verify_s3_files",
]