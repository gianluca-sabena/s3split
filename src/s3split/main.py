"""main: cli entry point"""
import os
import sys
import argparse
import threading
import signal
from distutils.util import strtobool

# This is the main file, only absolute path import are allowed here!!!
import s3split.s3util
import s3split.common
import s3split.actions


def parse_args(sys_args):
    """parse command line arguments"""
    def str2bool(val):
        return bool(strtobool(val))

    parser = argparse.ArgumentParser(prog='s3split', usage="%(prog)s [options] COMMAND [arguments]",
                                     description=('s3split splits big datasets in different tar archives'
                                                  ' and uploads/downloads them to/from S3 remote storage'))
    group_options = parser.add_argument_group('options', 'options common to all COMMAND')
    group_options.add_argument('--s3-secret-key', help='S3 secret key (can be set with env variable S3_SECRET_KEY)', default=os.environ.get('S3_SECRET_KEY', None), required=False)
    group_options.add_argument('--s3-access-key', help='S3 access key (can be set with env variable S3_ACCESS_KEY)', default=os.environ.get('S3_ACCESS_KEY', None), required=False)
    group_options.add_argument('--s3-endpoint', default=os.environ.get('S3_ENDPOINT', None),
                               help='S3 endpoint full hostname in the form http(s)://myhost:port (can be set with env variable S3_ENDPOINT)', required=False)
    # Boolean type does not work as expected... check https://stackoverflow.com/questions/15008758
    group_options.add_argument('--s3-verify-certificate', help='verfiy S3 endpoint tls certificate (can be set with env variable S3_VERIFY_CERTIFICATE)',
                               type=str2bool, default=os.environ.get('S3_VERIFY_CERTIFICATE', True))
    group_options.add_argument('--threads', help='Number of parallel threads ', type=int, default=5)
    group_options.add_argument('--stats-interval', help='Seconds between two stats print', type=int, default=30)
    subparsers = parser.add_subparsers(title='COMMAND', dest='command', required=True, help='%(prog)s [COMMAND] -h to see the full command help')
    # Upload
    parser_upload = subparsers.add_parser("upload", help="Split a dataset from source folder in multiple tar files and upload them to remote S3 target (upload -h to show more help)")
    parser_upload.add_argument('source', help="Local filesystem directory")
    parser_upload.add_argument('target', help="S3 path in the form s3://bucket/path (path is required!)")
    parser_upload.add_argument('-s', '--tar-size', help='Desired size in MB for a single split tar file', type=int, default=1024)
    parser_upload.add_argument('-d', '--description', help='Dataset description', required=False)
    # Download
    parser_download = subparsers.add_parser("download", help="Download dataset tar files from s3 source and join them in a local target folder (download -h to show more help)")
    parser_download.add_argument('source', help="S3 path in the form s3://bucket/path (path is required!)")
    parser_download.add_argument('target', help="Local filesystem directory")
    parser_download.add_argument('-p', '--prefix', help='folders and file name to restrict download to', required=False)
    # Check
    parser_check = subparsers.add_parser("check", help="Compare S3 metadata info (tar name and size) with remote S3 object (check -h to show more help)")
    parser_check.add_argument('target', help="S3 path in the form s3://bucket/...")
    # Parse s3 config from env vars
    args = parser.parse_args(sys_args)
    # print(args)
    for key in ['s3_secret_key', 's3_access_key', 's3_endpoint', 's3_verify_certificate']:
        if vars(args).get(key) is None:
            raise ValueError(f"Error! param --{key.replace('_','-')} or env variables {key.upper()} is required")
    return args

#
# --- main --- --- --- ---
#


def run_main(sys_args):
    """run main with sys args override, this allow tests"""
    logger = s3split.common.get_logger()
    event = threading.Event()

    # Catch ctrl+c
    def signal_handler(sig, frame):  # pylint: disable=unused-argument
        logger.info('You pressed Ctrl+C!... \n\nThe program will terminate AFTER ongoing file upload(s) will complete\n\n')
        # Send termination signal to threads
        event.set()
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Command
    try:
        args = parse_args(sys_args)
        # logger.info(f"Args: {args}")
        logger.info(f"Parallel threads: {args.threads}")
        action = s3split.actions.Action(args, event)
    except ValueError as ex:
        raise SystemExit(f"Error! {ex}")


def run_cli():
    """entry point for setup.py console script"""
    run_main(sys.argv[1:])


if __name__ == '__main__':
    run_cli()
