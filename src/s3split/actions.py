"""main actions: upload, check"""
import os
import time
import threading
import traceback
import concurrent.futures
from pprint import pformat
import tarfile
import tempfile
import s3split.s3util
import s3split.common
import s3split.common as com
import s3split.stats


class Action():
    """manage actions"""

    def __init__(self, args, event):
        self._args = args
        self._event = event
        self._logger = s3split.common.get_logger()
        self._executor = None
        if args.command == "upload":
            self.upload()
        elif args.command == "check":
            if not self.check(s3split.s3util.S3Uri(self._args.target)):
                raise ValueError("S3 check not passed")
        elif args.command == "download":
            if not self.check(s3split.s3util.S3Uri(self._args.source)):
                raise ValueError("S3 check not passed")
            self.download()

    def download(self):
        "download files from s3"
        def _run_download(tmpdir, s3_obj, s3_size, s3uri, stats_cb):
            def py_files(members):
                for tarinfo in members:
                    # Remove container path added if someone open the archive on a desktop
                    tarinfo.name = tarinfo.name.replace('s3split', '').strip('/')
                    if self._args.prefix is None:
                        yield tarinfo
                    elif self._args.prefix.strip('/') in tarinfo.name:
                        yield tarinfo
                    else:
                        self._logger.info(f"File skipped from untar (not in prefix {self._args.prefix.strip('/')}): {tarinfo.name}")
            tar_file = os.path.join(tmpdir, os.path.basename(s3_obj))
            self._logger.debug(f"(future) start download of s3 object '{s3_obj}' to local file '{tar_file}'")
            s3manager = s3split.s3util.S3Manager(self._args.s3_access_key, self._args.s3_secret_key, self._args.s3_endpoint,
                                                 self._args.s3_verify_certificate, s3uri.bucket, s3uri.object, stats_cb)
            if self._event.is_set():
                self._logger.warning(f"{s3_obj} - download interrupted because Ctrl + C was pressed!")
                return None
            with open(tar_file, 'wb') as file:
                self._logger.info(f"{s3_obj} downloading... ")
                s3manager.download_file(s3_obj, s3_size, file)
                file.close()
                self._logger.info(f"{s3_obj} download completed")
            tar = tarfile.open(tar_file)
            tar.extractall(path=self._args.target, members=py_files(tar))
            tar.close()
            self._logger.info(f"{s3_obj} archive extracted")
            self._logger.info(f"Active threads: {threading.active_count()}")
            return s3_obj

        # --- ---
        s3uri = s3split.s3util.S3Uri(self._args.source)
        if os.path.isdir(self._args.target):
            raise ValueError(f"download target directory '{self._args.target}' exsists... Please provide a new path!")
        if not os.path.isdir(self._args.target):
            try:
                os.makedirs(self._args.target)
            except OSError as ex:
                raise SystemExit(f"Creation of the directory {self._args.target} failed - {ex}")
            else:
                self._logger.info(f"Created download directory {self._args.target}")
        futures = {}
        downloaded = []
        s3_manager = s3split.s3util.S3Manager(self._args.s3_access_key, self._args.s3_secret_key, self._args.s3_endpoint,
                                              self._args.s3_verify_certificate, s3uri.bucket, s3uri.object, None)
        # check S3 connection...
        s3_manager.bucket_exsist()
        metadata = s3_manager.download_metadata()
        with tempfile.TemporaryDirectory() as tmpdir:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self._args.threads) as executor:
                splits = metadata.get("splits")
                tars = {tar.get('name'): tar.get('size') for tar in metadata.get("tars")}
                ids = s3split.common.split_searh_file(splits, self._args.prefix)

                # for tar in metadata["tars"]:
                #     future = executor.submit(_run_download, tmpdir, tar['name'], tar['size'], s3uri, stats.update)
                #     futures.update({future: tar['name']})
                if ids is not None and len(ids) > 0:
                    stats = s3split.stats.Stats(self._args.stats_interval, len(metadata['splits']), sum(c.get('size') for c in metadata.get('splits')))
                    for id in ids:
                        future = executor.submit(_run_download, tmpdir, s3split.common.gen_file_name(id), tars.get(s3split.common.gen_file_name(id)), s3uri, stats.update)
                        futures.update({future: s3split.common.gen_file_name(id)})
                    self._logger.debug(f"List of futures: {futures}")
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            data = future.result()
                            downloaded.append(data)
                            self._logger.debug(f"(future) completed - data: {data}")
                        except Exception as exc:  # pylint: disable=broad-except
                            self._logger.error(f"(future) generated an exception: {exc}")
                            traceback_str = traceback.format_exc(exc)
                            self._logger.error(f"(future) generated an exception: {traceback_str}")
                    stats.print()
                else:
                    self._logger.info(f"No split id selected")

    def upload(self):
        """upload splits to s3"""
        def _run_upload(split, s3uri, stats_cb):
            """create a tar and upload"""
            def tar_filter(tobj):
                # Add a container path if someone open the archive on a desktop
                new = tobj.name.replace(self._args.source.strip('/'), 's3split').strip('/')
                tobj.name = new
                return tobj

            name_tar = s3split.common.gen_file_name(split.get('id'))
            self._logger.debug(f"(future) start archive/upload for tar {name_tar}")
            s3manager = s3split.s3util.S3Manager(self._args.s3_access_key, self._args.s3_secret_key, self._args.s3_endpoint,
                                                 self._args.s3_verify_certificate, s3uri.bucket, s3uri.object, stats_cb)
            # Filter function to update tar path, required to untar in a safe location
            with tempfile.TemporaryDirectory() as tmpdir:
                tar_file = os.path.join(tmpdir, name_tar)
                # Start tar
                if not self._event.is_set():
                    self._logger.info(f"{name_tar} archive creating... ")
                    with tarfile.open(tar_file, "w") as tar:
                        for path in split.get('paths'):
                            # remove base path from folder with filter function
                            tar.add(os.path.join(self._args.source, path), filter=tar_filter)
                        tar.close()
                    self._logger.info(f"{name_tar} archive completed")
                # Start upload
                if self._event.is_set():
                    self._logger.warning(f"{name_tar} - archive/upload interrupted because Ctrl + C was pressed!")
                    return None
                self._logger.info(f"{name_tar} uploading... ")
                s3manager.upload_file(tar_file)
                self._logger.info(f"{name_tar} upload completed")
                self._logger.info(f"Active threads: {threading.active_count()}")
                return {"name": os.path.basename(tar_file),
                        "id": split.get('id'), "size": os.path.getsize(tar_file)}

        # --- --- ---
        if not os.path.isdir(self._args.source):
            raise ValueError(f"upload source: '{self._args.source}' is not a directory")
        self._logger.info(f"Tar object max size: {self._args.tar_size} MB")
        self._logger.info(f"Print stats evry: {self._args.stats_interval} seconds")
        if self._args.description is None or len(self._args.description) == 0:
            self._logger.warning(f"No description provided!!! Please use upload -d 'description' ... ")
        s3uri = s3split.s3util.S3Uri(self._args.target)
        s3_manager = s3split.s3util.S3Manager(self._args.s3_access_key, self._args.s3_secret_key, self._args.s3_endpoint,
                                              self._args.s3_verify_certificate, s3uri.bucket, s3uri.object, None)
        s3_manager.bucket_exsist()
        # Check if bucket is empty and if a metadata file is present
        objects = s3_manager.list_bucket_objects()
        if objects is not None and len(objects) > 0:
            self._logger.warning(f"Remote S3 bucket is not empty!!!!!")
            metadata = s3_manager.download_metadata()
            if metadata is not None and len(metadata.get('splits')) > 0:
                self._logger.warning("Remote S3 bucket contains a metadata file!")
                # TODO: If there is a remote metadata? exit and force user to clean bucket?
        # Upload metadata file
        splits = s3split.common.split_file_by_size(self._args.source, self._args.tar_size * 1024 * 1024)
        # self._logger.debug(f"Splits: {splits}")
        stats = s3split.stats.Stats(self._args.stats_interval, len(splits), sum(c.get('size') for c in splits))
        tars_uploaded = []
        future_split = {}
        if not s3_manager.upload_metadata(splits, None, self._args.description):
            self._logger.error("Metadata json file upload failed!")
            raise SystemExit
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._args.threads) as executor:
            for split in splits:
                future = executor.submit(_run_upload, split, s3uri, stats.update)
                future_split.update({future: split.get('id')})
            self._logger.debug(f"List of futures: {future_split}")
            for future in concurrent.futures.as_completed(future_split):
                try:
                    data = future.result()
                    tars_uploaded.append(data)
                    self._logger.debug(f"(future) completed - data: {data}")
                except Exception as exc:  # pylint: disable=broad-except
                    self._logger.error(f"Future generated an exception: {exc}")
                    traceback_str = traceback.format_exc(exc)
                    self._logger.error(f"Future generated an exception: {traceback_str}")
        if not s3_manager.upload_metadata(splits, tars_uploaded, self._args.description):
            raise SystemExit("Metadata json file upload failed!")
        stats.print()

    def check(self, s3uri):
        """download splits to s3"""
        self._logger.info(f"Check S3 - Compare S3 metadata info (tar name and size) with remote S3 object")
        s3_manager = s3split.s3util.S3Manager(self._args.s3_access_key, self._args.s3_secret_key, self._args.s3_endpoint,
                                              self._args.s3_verify_certificate, s3uri.bucket, s3uri.object, None)
        metadata = s3_manager.download_metadata()
        self._logger.info(f"Metadata from S3:\n{pformat(metadata)}")
        errors = False
        if metadata is None:
            self._logger.info(f"Metadata file not found on S3 enpoint s3://{s3uri.bucket}/{s3uri.object}")
            return True
        metadata_tar = {tar['name']: tar['size'] for tar in metadata.get("tars") if tar is not None}
        s3objects = s3_manager.list_bucket_objects()
        s3_data = {obj['Key']: obj['Size'] for obj in s3objects}
        for split in metadata.get("splits"):
            if split is None:
                errors = True
                self._logger.error(f"Metadata file is corrupted! Split array is incomplete!")
            else:
                key = os.path.join(s3uri.object, s3split.common.gen_file_name(split.get('id')))
                # self._logger.info(f"S3 size: {s3_data.get(key)}, Tar size: {metadata_tar.get(s3split.common.gen_file_name(split.get('id')))}")
                if s3_data.get(key) is None:
                    self._logger.error(f"Split part {key} not found on S3! Inclomplete uploads detected!")
                    errors = True
                elif s3_data.get(key) != metadata_tar.get(s3split.common.gen_file_name(split.get('id'))):
                    errors = True
                    self._logger.error(f"Check size for split part {key} failed! Expected size: {split.get('size')} comparade to s3 object size: {s3_data.get(key)} ")
                else:
                    self._logger.debug(f"Check size for split part {key}: OK")
        if not errors:
            self._logger.info("Check S3 passed (all objects are present and have a size equal to metadata info)")
        dirs = [f"    - {dir}\n" for dir in sorted(s3split.common.split_get_dirs(metadata.get("splits")))]
        self._logger.info(f"Print dataset direcotries from metadata:\n----------\n{''.join(dirs)}----------\n")
        return not errors
