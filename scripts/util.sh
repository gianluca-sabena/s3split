#!/bin/bash
#
#   REMEMBER TO check syntax with https://github.com/koalaman/shellcheck
#

#set -x          # debug enabled
set -e          # exit on first error
set -o pipefail # exit on any errors in piped commands

#ENVIRONMENT VARIABLES
declare SCRIPT_PATH=""
SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
declare CURRENT_PATH
CURRENT_PATH=$(pwd)
declare APP_NAME="s3split"
MINIO_SERVER_DATA="/tmp/minio-server/data"
MINIO_ENDPOINT="http://127.0.0.1:9000"
MINIO_ACCESS_KEY="test_access"
MINIO_SECRET_KEY="test_secret"
PATH_TEST_FILES="/tmp/s3cmd-test-files"
# @info:  Parses and validates the CLI arguments
# @args:	Global Arguments $@

function parseCli() {
  if [[ "$#" -eq 0 ]]; then
      echo "  ${0}: {command}"
      echo ""
      echo "  command:"
      echo "    - create-pipenv-dev"
      echo "    - test-pip-install"
      echo "    - run-python"
      echo "    - run-cli"
      echo "    - run-test"
      echo ""
      echo "    - start-minio-server               start minio server "
      echo ""
      echo "    - generate-data"
      echo "    - test-s3split-local-upload"
      echo "    - test-s3split-remote-upload"
      exit 0
  fi
  while [[ "$#" -gt 0 ]]; do
    declare KEY="$1"
    # declare VALUE="$2"
    case "${KEY}" in
    # exec command here
    create-pipenv-dev)
      cd  "${SCRIPT_PATH}/../"
      # install current package in editable mode (use simlink to source code)
      # https://setuptools.readthedocs.io/en/latest/setuptools.html#development-mode
      # https://pipenv-fork.readthedocs.io/en/latest/basics.html#editable-dependencies-e-g-e
      pipenv install --dev
      echo ""
      echo "========== Test run cli: pipenv run ${APP_NAME} ========== "
      pipenv run ${APP_NAME}
      echo ""
      echo "========== Test run module: pipenv run python -m helloworld.main ========== "
      pipenv run python -m helloworld.main
      echo ""
    ;;
    test-pip-install)
      export TMP_VENV_PATH="/tmp/pipenv/${APP_NAME}"
      echo ""
      echo "========== Create venv in temp folder: ${TMP_VENV_PATH} ========== "
      export PIPENV_IGNORE_VIRTUALENVS=1
      export PIPENV_VENV_IN_PROJECT="enabled"
      # export DISTUTILS_DEBUG="enabled"
      [ -d "${TMP_VENV_PATH}" ] && rm -rf "${TMP_VENV_PATH}"
      mkdir -p "${TMP_VENV_PATH}"
      cd "${TMP_VENV_PATH}"
      pipenv --python 3.7
      echo "========== Install ${APP_NAME} package name from source ${SCRIPT_PATH}/../setup.py ========== "
      pipenv run pip install "${SCRIPT_PATH}/../"
      echo ""
      echo "========== Test run cli: pipenv run ${APP_NAME} ========== "
      pipenv run ${APP_NAME}
      echo ""
      echo "========== Test run module: pipenv run python -m helloworld.main ========== "
      pipenv run python -m helloworld.main
      echo ""
    ;;
    run-python)
      cd "${SCRIPT_PATH}/../"
      # pipenv run pip install -e .
      pipenv run python "${SCRIPT_PATH}/../src/${APP_NAME}/main.py"
    ;;
    run-cli)
      cd "${SCRIPT_PATH}/../"
      # pipenv run pip install -e .
      pipenv run ${APP_NAME}
    ;;
    run-test)
      cd "${SCRIPT_PATH}/../"
      pipenv run pytest -v
    ;;
    publish-private)
      # publish package to private repository
      # example --- ~/.pypirc ----
      # [distutils]
      # index-servers = private
      # [private]
      # repository: https://...
      # username: ...
      # password: ....
      # ca_cert: /.../ca.pem
      pipenv run python setup.py sdist bdist_wheel
      if [[ ! -f "${HOME}/.pypirc" ]];then
        echo "Create ~/.pypirc with private python repository server..."
        exit 1
      fi
      pipenv run twine upload -r private dist/*
    ;;
    start-minio-server)
      # echo "Key: ${KEY} - Value: ${VALUE}"
      # echo "Script dir is: ${SCRIPT_PATH}"
      echo "Start minio server... data path: ${MINIO_SERVER_DATA} - access_key: ${MINIO_ACCESS_KEY} - secret key: ${MINIO_SECRET_KEY} "
      export MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY}
      export MINIO_SECRET_KEY=${MINIO_SECRET_KEY}
      minio server ${MINIO_SERVER_DATA}
      ;;
    generate-data)
      # Generate 5 GB + 1 GB
      local SIZE=1024
      local NUM_FILES=128
      local NUM_FOLDERS=40
      local TOTAL=$(( SIZE * NUM_FILES * NUM_FOLDERS))
      if [[ ! -d "${PATH_TEST_FILES}" ]]; then
        mkdir -p "${PATH_TEST_FILES}"
        local counter=1
        while [[ $counter -le $NUM_FOLDERS ]]; do
          genfilesrandom ${PATH_TEST_FILES}/dir_${counter} ${SIZE} ${NUM_FILES}
          ((counter += 1))
        done
        # To do generate 8 files * 128 Mb = 1 GB
        local SIZE=$(( 1024 * 128 ))
        genfilesrandom ${PATH_TEST_FILES} ${SIZE} 8
        local TOTAL; TOTAL=$(du -sh ${PATH_TEST_FILES})
        echo "Generated total KB: ${TOTAL}"
      else
        echo "Random data already present in ${PATH_TEST_FILES}"
      fi

    ;;
    test-s3split-local-upload)
      echo "Run s3split with local minio"
      "./${0}" generate-data
      python "${SCRIPT_PATH}/../src/s3split/main.py" --s3-secret-key ${MINIO_SECRET_KEY} --s3-access-key ${MINIO_ACCESS_KEY} --s3-endpoint ${MINIO_ENDPOINT} --threads 4 upload "${PATH_TEST_FILES}" "s3://s3split/cli-test-1" --tar-size 500
    ;;
    test-s3split-remote-upload)
      echo "Run s3split with remote minio"
      # shellcheck disable=SC1091,SC1090
      source "${HOME}/.s3split"
      python "${SCRIPT_PATH}/../src/s3split/main.py" --s3-secret-key "${S3_SECRET_KEY}" --s3-access-key "${S3_ACCESS_KEY}" --s3-endpoint "${S3_ENDPOINT}" --s3-verify-ssl false --threads 4 upload "${PATH_TEST_FILES}" "s3://${S3_BUCKET}/s3split-test" --tar-size 500
    ;;
    -h | *)
      ${0}
      ;;
    esac
    shift
  done
  cd "${CURRENT_PATH}"
}


#
# genfilesrandom FOLDER SIZE_KB NUM_FILES
#
function genfilesrandom() {
  local DEST_PATH=${1}
  local SIZE=$((1024 * $2))
  local NUM_FILES=$3
  mkdir -p "${DEST_PATH}"
  echo "Dest folder: ${DEST_PATH}"
  echo "Size kb: ${SIZE}"
  echo "Number of files: ${NUM_FILES}"
  echo "Creating master file..."
  head -c "$SIZE" /dev/urandom >"${DEST_PATH}/file_1.txt"
  local counter=2
  while [[ $counter -le $NUM_FILES ]]; do
    echo "Duplicating file: $counter "
    cp "${DEST_PATH}/file_1.txt" "${DEST_PATH}/file_${counter}.txt"
    ((counter += 1))
  done
}
parseCli "$@"
