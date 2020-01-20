"Unit test"
from pprint import pformat
import pytest
import s3split.common
import s3split.main
import s3split.s3util
import common

LOGGER = s3split.common.get_logger()


@pytest.mark.s3
def test_s3_uri():
    "parse valid s3 string"
    s3_uri = s3split.s3util.S3Uri("s3://aaa/bbb")
    test = s3_uri.bucket == "aaa" and s3_uri.object == "bbb"
    assert test


@pytest.mark.s3
def test_s3_uri_long():
    "parse valid long s3 string"
    s3_uri = s3split.s3util.S3Uri("s3://aaa/bbb/ccc/ddd")
    test = s3_uri.bucket == "aaa" and s3_uri.object == "bbb/ccc/ddd"
    assert test


@pytest.mark.s3
def test_s3_uri_missing_path():
    "test s3 uri without path"
    with pytest.raises(SystemExit, match=r'S3 URI must contains bucket and path s3://bucket/path'):
        assert s3split.s3util.S3Uri("s3://aaa")


@pytest.mark.s3
def test_s3_list_bucket():
    "list bucket"
    s3_manager = s3split.s3util.S3Manager(common.MINIO_ACCESS_KEY, common.MINIO_SECRET_KEY, common.MINIO_ENDPOINT,
                                          common.MINIO_VERIFY_SSL, common.MINIO_BUCKET, common.MINIO_PATH)
    # s3_manager.bucket_exsist()
    # s3_manager.create_bucket()
    objects = s3_manager.list_bucket_objects()
    LOGGER.info(pformat(objects))


@pytest.mark.s3
def test_s3_get_metadata():
    "full s3 operation"
    s3_manager = s3split.s3util.S3Manager(common.MINIO_ACCESS_KEY, common.MINIO_SECRET_KEY, common.MINIO_ENDPOINT,
                                          common.MINIO_VERIFY_SSL, common.MINIO_BUCKET, common.MINIO_PATH)
    metadata = s3_manager.download_metadata()
    LOGGER.info(pformat(metadata))
