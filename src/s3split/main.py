"""main: cli entry point"""
import sys
import argparse
import threading
import signal

# This is the main file, only absolute path import are allowed here!!!
import s3split.s3util
import s3split.common
import s3split.actions


def parse_args(sys_args):
    """parse command line arguments"""
    parser = argparse.ArgumentParser(prog='s3split',
                                     description=('s3split splits big datasets in different tar archives'
                                                  ' and uploads/downloads them to/from s3 remote storage'))
    parser.add_argument('--s3-secret-key', help='S3 secret key', required=True)
    parser.add_argument('--s3-access-key', help='S3 access key', required=True)
    parser.add_argument('--s3-endpoint', help='S3 endpoint full hostname in the form http(s)://myhost:port', required=True)
    parser.add_argument('--s3-verify-ssl', help='verfiy S3 endpoint ssl certificate', required=False, default=True)
    parser.add_argument('--threads', help='Number of parallel threads ', required=False, type=int, default=5)
    parser.add_argument('--stats-interval', help='Seconds between two stats print', required=False, type=int, default=30)
    subparsers = parser.add_subparsers(dest='action')
    # Upload
    parser_upload = subparsers.add_parser("upload", help="upload -h show more help")
    parser_upload.add_argument('source', help="Local filesystem directory")
    parser_upload.add_argument('target', help="S3 path in the form s3://bucket/path (path is required!)")
    parser_upload.add_argument('--tar-size', help='Max size in MB for a single split tar file', required=False, type=int, default=500)
    # Download
    parser_download = subparsers.add_parser("download", help="download -h show more help")
    parser_download.add_argument('source', help="S3 path in the form s3://bucket/path (path is required!)")
    parser_download.add_argument('target', help="Local filesystem directory")
    # Check
    parser_check = subparsers.add_parser("check", help="check -h show more help")
    parser_check.add_argument('target', help="S3 path in the form s3://bucket/...")
    return parser.parse_args(sys_args)

#
# --- main --- --- --- ---
#


def run_main(sys_args):
    """run main with sys args override, this allow tests"""
    logger = s3split.common.get_logger()
    args = parse_args(sys_args)
    action = s3split.actions.Action(args)
    event = threading.Event()

    def signal_handler(sig, frame):  # pylint: disable=unused-argument
        logger.info('You pressed Ctrl+C!... \n\nThe program will terminate AFTER ongoing file upload(s) complete\n\n')
        # Send termination signal to threads
        action.stop()
    # Catch ctrl+c
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    #logger.info(f"Action: {args.action} {args.source} {args.target}")
    logger.info(f"Parallel threads: {args.threads}")
    try:
        if args.action == "upload":
            action.upload()
        elif args.action == "check":
            if not action.check():
                raise SystemExit("Check not passed!")
        elif args.action == "download":
            # if not action.check():
            #     logger.error("s3 metadata file error can not download parts")
            #     raise SystemExit()
            action.download()
    except ValueError as ex:
        logger.error(f"ValueError: {ex}")
        raise ValueError(ex)


def run_cli():
    """entry point for setup.py console script"""
    run_main(sys.argv[1:])


if __name__ == '__main__':
    run_cli()
