"""S3 utility: connection manager, s3 url"""
import os
import threading
import json
import re
from urllib.parse import urlparse
import urllib3
import boto3
import boto3.session
import botocore
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
import s3split.common

# logger = s3split.common.get_logger()
urllib3.disable_warnings()

# From https://github.com/s3tools/s3cmd/blob/master/S3/S3Uri.py


class S3Uri():
    """Contain a s3 url s3://bucket/object"""
    _re = re.compile("^s3:///*([^/]*)/?(.*)", re.IGNORECASE | re.UNICODE)

    def __init__(self, string):
        self.bucket = None
        self.object = None
        match = self._re.match(string)
        if not match:
            raise ValueError("%s: not a S3 URI" % string)
        groups = match.groups()
        self.bucket = groups[0]
        self.object = groups[1]
        if len(self.object) == 0:
            raise SystemExit(f"S3 URI must contains bucket and path s3://bucket/path")


class ProgressPercentage(object):
    """progress upload callback from boto3"""
    files = dict()

    def __init__(self, cb_stats_update, filename, size):
        self._filename = filename
        self._cb_stats_update = cb_stats_update
        self._size = float(size)
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            if callable(self._cb_stats_update):
                self._cb_stats_update(self._filename, bytes_amount, self._size)


class S3Manager():
    """Manage S3 connection with boto3"""

    def __init__(self, s3_access_key, s3_secret_key, s3_endpoint, s3_verify_ssl, s3_bucket, s3_path, cb_stats_update=None):
        #self._logger = s3split.common.get_logger()
        self._cb_stats_update = cb_stats_update
        self._session = boto3.session.Session()
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self._s3_client = None
        # create client from session (thread safe)
        try:
            url = urlparse(s3_endpoint)
            self._s3_use_ssl = False
            if url.scheme == "https":
                self._s3_use_ssl = True
            self._s3_client = self._session.client('s3', aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key,
                                                   endpoint_url=s3_endpoint, use_ssl=self._s3_use_ssl, verify=s3_verify_ssl,
                                                   config=botocore.config.Config(max_pool_connections=25))
        except ValueError as ex:
            raise ValueError(f"S3 ValueError: {ex}")
        except ClientError as ex:
            raise ValueError(f"S3 ClientError: {ex}")

    def _wrap_exception(self, ex):
        "exit when detect a fatal client exception"
        raise SystemExit(f"Fatal boto3 exception: {ex}")

    def get_client(self):
        """return boto3 client"""
        return self._s3_client

    def create_bucket(self):
        """create a bucket"""
        if self.bucket_exsist():
            return True
        try:
            self._s3_client.create_bucket(Bucket=self.s3_bucket)
            return True
        except ClientError as ex:
            self._wrap_exception(ex)

    def bucket_exsist(self):
        """Check if S3 bucket exsist

        :param bucket_name: Bucket to create
        :return: True if bucket is present, else False
        """
        # Check if a bucket exsists
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/migrations3.html?highlight=clienterror#accessing-a-bucket
        try:
            self._s3_client.head_bucket(Bucket=self.s3_bucket)
            return True
        except botocore.exceptions.ParamValidationError as ex:
            self._wrap_exception(ex)
        except ClientError as ex:
            # If it was a 404 error, then the bucket does not exist.
            if ex.response['Error']['Code'] == '404':
                return False
            self._wrap_exception(ex)

    def list_bucket_objects(self):
        """List the objects in an Amazon S3 bucket

        :param bucket_name: string
        :return: List of bucket objects
        """

        # Retrieve the list of bucket objects
        try:
            response = self._s3_client.list_objects_v2(Bucket=self.s3_bucket, Prefix=self.s3_path)
            # Only return the contents if we found some keys
            if response['KeyCount'] > 0:
                return response['Contents']
            return None
        except ClientError as ex:
            self._wrap_exception(ex)

    def upload_metadata(self, splits=None, tars=None):
        """upload metadata file in json format"""
        content = {
            "info": {
                "hostname": "",
                "uname": "",
                "id": ""
            },
            "tars": tars,
            "splits": splits}
        if not self.bucket_exsist():
            self.create_bucket()
        try:
            self._s3_client.put_object(Bucket=self.s3_bucket, Key=self.s3_path+'/s3split-metadata.json', Body=json.dumps(content))
            return True
        except ClientError as ex:
            self._wrap_exception(ex)

    def download_metadata(self):
        """download metadata and parse json"""
        try:
            stream = self._s3_client.get_object(Bucket=self.s3_bucket, Key=self.s3_path+'/s3split-metadata.json')
            if stream is not None:
                data = stream['Body'].read().decode('utf-8')
                return json.loads(data)
        except ClientError as ex:
            self._wrap_exception(ex)

    def download_file(self, s3_object, s3_size, file):
        """download object from s3"""
        full_path = os.path.join(self.s3_path, s3_object)
        progress = ProgressPercentage(self._cb_stats_update, full_path, s3_size)
        config = TransferConfig(multipart_threshold=1024 * 1024 * 64, max_concurrency=15,
                                multipart_chunksize=1024 * 1024 * 64, use_threads=True)
        try:
            self._s3_client.download_fileobj(self.s3_bucket, full_path, file, Config=config, Callback=progress)
            return full_path
        except ClientError as ex:
            self._wrap_exception(ex)

    def upload_file(self, fs_path):
        """upload a single file with multiple parallel (concurrency) worlkers"""
        config = TransferConfig(multipart_threshold=1024 * 1024 * 64, max_concurrency=15,
                                multipart_chunksize=1024 * 1024 * 64, use_threads=True)
        final_path = self.s3_path+'/'+os.path.basename(fs_path)
        progress = ProgressPercentage(self._cb_stats_update, fs_path, os.path.getsize(fs_path))
        try:
            self._s3_client.upload_file(fs_path, self.s3_bucket, final_path,
                                        Config=config,
                                        Callback=progress
                                        )
        except ClientError as ex:
            self._wrap_exception(ex)
