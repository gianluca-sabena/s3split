
import os
import sys
import pytest
import common

# Add src path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

# Define all fixture here
@pytest.fixture(scope="module")
def docker_minio_fixture():
    """start docker with minio"""
    common.docker_minio_fixture()
