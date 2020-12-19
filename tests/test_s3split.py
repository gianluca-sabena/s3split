# pylint: disable=missing-function-docstring,unused-argument,redefined-outer-name
"""test full application"""
from pathlib import Path
from pprint import pformat
from filecmp import dircmp
import os
import tempfile
import pytest
import s3split.common
import s3split.main
import s3split.s3util
import common

LOGGER = s3split.common.get_logger()


@pytest.mark.args
def test_minio_invalid_s3uri(docker_minio_fixture):
    """test minio connection error"""
    with pytest.raises(SystemExit, match=r'S3 URI must contains bucket and path s3://bucket/path'):
        assert s3split.main.run_main(["--s3-secret-key", "A", "--s3-access-key", "B", "--s3-endpoint",
                                      "C", "upload", "/tmp", "s3://aaa", "-d", "description"])


@pytest.mark.args
def test_argparse_invalid_local_path(docker_minio_fixture):
    """test that exception is raised for invalid local path"""
    with pytest.raises(SystemExit, match=r"upload source: '/D' is not a directory"):
        assert s3split.main.run_main(["--s3-secret-key", common.MINIO_SECRET_KEY, "--s3-access-key", common.MINIO_ACCESS_KEY,
                                      "--s3-endpoint", common.MINIO_ENDPOINT, "upload", "/D", f"s3://{common.MINIO_BUCKET}/{common.MINIO_PATH}", "-d", "description"])


@pytest.mark.args
def test_minio_invalid_endpoint(docker_minio_fixture):
    """test minio connection error"""
    with pytest.raises(SystemExit, match=r"Error! S3 validation - Invalid endpoint: C"):
        assert s3split.main.run_main(["--s3-secret-key", "A", "--s3-access-key", "B",
                                      "--s3-endpoint", "C", "upload", "/tmp", "s3://aaa/bbb", "-d", "description"])


@pytest.mark.args
def test_minio_invalid_bucket(docker_minio_fixture):
    with pytest.raises(SystemExit, match=r'S3 URI must contains bucket and path s3://bucket/path'):
        assert s3split.main.run_main(["--s3-secret-key", common.MINIO_SECRET_KEY, "--s3-access-key", common.MINIO_ACCESS_KEY,
                                      "--s3-endpoint", common.MINIO_ENDPOINT, "upload", "/tmp", "s3://ddd/", "-d", "description"])


@pytest.mark.last
def test_upload_check_download(docker_minio_fixture):
    size = 1024
    with tempfile.TemporaryDirectory() as tmpdir:
        # tmpdir = "/tmp/test"
        path_upload = f"{tmpdir}/upload"
        path_download = f"{tmpdir}/download"
        common.generate_random_files(path_upload, 10, size)
        common.generate_random_files(path_upload+"/dir_1", 4, size)
        common.generate_random_files(path_upload+"/dir_2/dir_a", 8, size)
        common.generate_random_files(path_upload+"/dir_2/dir_b", 8, size)
        s3split.main.run_main(["--s3-secret-key", common.MINIO_SECRET_KEY, "--s3-access-key", common.MINIO_ACCESS_KEY,
                               "--s3-endpoint", common.MINIO_ENDPOINT, "--threads", "2", "--stats-interval", "1",
                               "upload", path_upload, f"s3://{common.MINIO_BUCKET}/{common.MINIO_PATH}", "--tar-size", "5", "-d", "description"])
        # s3split.main.run_main(["--s3-secret-key", common.MINIO_SECRET_KEY, "--s3-access-key", common.MINIO_ACCESS_KEY,
        #                        "--s3-endpoint", common.MINIO_ENDPOINT,
        #                        "check", f"s3://{common.MINIO_BUCKET}/{common.MINIO_PATH}"])
        s3split.main.run_main(["--s3-secret-key", common.MINIO_SECRET_KEY, "--s3-access-key", common.MINIO_ACCESS_KEY,
                               "--s3-endpoint", common.MINIO_ENDPOINT, "--threads", "2", "--stats-interval", "1",
                               "download", f"s3://{common.MINIO_BUCKET}/{common.MINIO_PATH}", os.path.join(path_download, "full")])
        # Download with search prefix
        s3split.main.run_main(["--s3-secret-key", common.MINIO_SECRET_KEY, "--s3-access-key", common.MINIO_ACCESS_KEY,
                               "--s3-endpoint", common.MINIO_ENDPOINT, "--threads", "2", "--stats-interval", "1",
                               "download", f"s3://{common.MINIO_BUCKET}/{common.MINIO_PATH}", os.path.join(path_download, "search"), "--prefix", "dir_2/dir_a"])
        # Compare dirs
        upload_set = set(sorted(Path(path_upload).rglob('**/*.*')))
        download_full_set = set(sorted(Path(os.path.join(path_download, "full")).rglob('**/*.*')))
        download_search_set = set(sorted(Path(os.path.join(path_download, "search")).rglob('**/*.*')))
        LOGGER.info(f"Compare {upload_set - download_full_set}")
        # TODO: path is absolute path so compare do not works, loop and create relative path so upload_set - download_full_set == set()
        assert len(upload_set) == len(download_full_set) and len(download_search_set) == 8


@pytest.mark.full
def test_check_s3_error(docker_minio_fixture):
    n_files = 10
    size = 1024
    with tempfile.TemporaryDirectory() as tmpdir:
        path_upload = f"{tmpdir}/upload"
        common.generate_random_files(path_upload, n_files, size)
        s3split.main.run_main(["--s3-secret-key", common.MINIO_SECRET_KEY, "--s3-access-key", common.MINIO_ACCESS_KEY,
                               "--s3-endpoint", common.MINIO_ENDPOINT, "--threads", "2", "--stats-interval", "1",
                               "upload", path_upload, f"s3://{common.MINIO_BUCKET}/{common.MINIO_PATH}", "--tar-size", "10"])
        s3_manager = s3split.s3util.S3Manager(common.MINIO_ACCESS_KEY, common.MINIO_SECRET_KEY, common.MINIO_ENDPOINT,
                                              common.MINIO_VERIFY_SSL, common.MINIO_BUCKET, common.MINIO_PATH)
        open(os.path.join(path_upload, "s3split-part-1.tar"), 'a').close()
        s3_manager.upload_file(os.path.join(path_upload, "s3split-part-1.tar"))
        with pytest.raises(SystemExit, match=r'Error! S3 check not passed'):
            s3split.main.run_main(["--s3-secret-key", common.MINIO_SECRET_KEY, "--s3-access-key", common.MINIO_ACCESS_KEY,
                                   "--s3-endpoint", common.MINIO_ENDPOINT,
                                   "check", f"s3://{common.MINIO_BUCKET}/{common.MINIO_PATH}"])
