"Unit test"
import tempfile
import subprocess
import os
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
def test_s3_get_metadata():
    "full s3 operation"
    s3_manager = s3split.s3util.S3Manager(common.MINIO_ACCESS_KEY, common.MINIO_SECRET_KEY, common.MINIO_ENDPOINT,
                                          common.MINIO_VERIFY_SSL, common.MINIO_BUCKET, common.MINIO_PATH)
    s3_manager.upload_metadata(['A'], ['B'], 'X"y\'a')
    metadata = s3_manager.download_metadata()
    objects = s3_manager.list_bucket_objects()
    # LOGGER.info(pformat(metadata))
    # LOGGER.info(pformat(objects))
    assert metadata.get('splits') == ['A'] and metadata.get('tars') == ['B'] and metadata.get('description') == 'X"y\'a'


@pytest.mark.file
def test_split_new():
    "split files"
    with tempfile.TemporaryDirectory() as tmpdir:
        open(os.path.join(tmpdir, "test.txt"), 'a').close()
        dirs = ["dir_a_1", "dir_a_1/dir_b_1", "dir_a_1/dir_b_2"]
        for dir in dirs:
            os.mkdir(os.path.join(tmpdir, dir))
            common.generate_random_files(os.path.join(tmpdir, dir), 8, 12)
        splits = s3split.common.split_file_by_size(tmpdir, 50 * 1024)
        ids_file = s3split.common.split_searh_file(splits, "dir_a_1/dir_b_1/file_1.txt")
        ids_folder = s3split.common.split_searh_file(splits, "dir_a_1/dir_b_1")
        ids_all = s3split.common.split_searh_file(splits)
        split_dirs = s3split.common.split_get_dirs(splits)
        # LOGGER.info(subprocess.check_output(['tree', tmpdir]).decode('utf8'))
        # LOGGER.info(pformat(splits))
        # LOGGER.info(f"Files: {ids_file} Folders: {ids_folder} All:{sorted(ids_all)} Directories: {sorted(split_dirs)} {sorted(dirs)}")
        assert ids_file == [6] and ids_folder == [5, 6] and sorted(split_dirs) == sorted(dirs) and ids_all == [i+1 for i in range(6)]
