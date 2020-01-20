"shared test functions"
import os
import docker
import s3split.common

LOGGER = s3split.common.get_logger()

DOCKER_MINIO_IMAGE = "minio/minio:RELEASE.2019-10-12T01-39-57Z"
MINIO_ACCESS_KEY = "test_access"
MINIO_SECRET_KEY = "test_secret"
MINIO_ENDPOINT = "http://127.0.0.1:9000"
MINIO_VERIFY_SSL = "false"
MINIO_BUCKET = "s3split"
MINIO_PATH = "test"


def docker_minio_fixture():
    """start docker container with minio"""
    # TODO: add a check to pull image if not present
    client = docker.from_env()
    # Check if minio is running
    LOGGER.info("--- docker_minio_fixture")
    try:
        minio = client.containers.get('minio')
    except docker.errors.NotFound:
        LOGGER.info("Container minio not found... creating...")
        minio = client.containers.create(DOCKER_MINIO_IMAGE, 'server /tmp', ports={'9000/tcp': 9000}, detach=True, name="minio",
                                         environment={"MINIO_ACCESS_KEY": MINIO_ACCESS_KEY, "MINIO_SECRET_KEY": MINIO_SECRET_KEY})
    if minio.status != 'running':
        LOGGER.info("Container minio not running... starting...")
        minio.start()
    return True


def generate_random_files(full_path, n_files, size):
    """path, n_files, number of files file size in kb"""
    if not os.path.isdir(full_path):
        try:
            os.makedirs(full_path)
        except OSError as ex:
            LOGGER.error(f"Creation of the directory {full_path} failed - {ex}")
        else:
            LOGGER.info(f"Successfully created the directory {full_path}")
    for i in range(n_files):
        with open(os.path.join(full_path, f"file_{i+1}.txt"), 'wb') as fout:
            fout.write(os.urandom(size * 1024))
