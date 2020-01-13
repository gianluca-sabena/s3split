"""common functions: logging, split files"""
import logging
import random
import os


def get_logger():
    """get a logger"""
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%H:%M:%S', level=logging.INFO)
    return logging

def gen_file_name(split_id):
    """generate split tar filename"""
    return f"s3split-part-{split_id}.tar"

def split_file_by_size(path, max_size):
    """split first deep level objects (files or folder) in different tars with a maximum size)"""
    if not os.path.isdir(path):
        raise ValueError("path argument is not a valid directory")
    def get_path_size(start_path):
        total_size = 0
        if os.path.isfile(start_path):
            return os.path.getsize(start_path)
        for dirpath, _, filenames in os.walk(start_path):
            for file in filenames:
                path = os.path.join(dirpath, file)
                # skip if it is symbolic link
                if not os.path.islink(path):
                    total_size += os.path.getsize(path)
        return total_size
    splits = []
    tar_size = 0
    tar_paths = []
    list_obj = [os.path.join(path, f) for f in os.listdir(path)]
    random.shuffle(list_obj)
    split_id = 1
    for obj in list_obj:
        size = get_path_size(obj)
        # logger.debug(f"path: {p} - size: {size} - {tar_size + size} - {max_size * 1024 * 1024}")
        if size > (max_size * 1024 * 1024):
            raise SystemExit(f"Single path '{obj}' has a size bigger then max allowed split size. Exit")
        if (tar_size + size) <= (max_size * 1024 * 1024):
            tar_size += size
            tar_paths.append(obj)
        else:
            splits.append({'paths': tar_paths, 'size': tar_size, 'id': split_id})
            split_id += 1
            tar_size = size
            tar_paths = [obj]
    if tar_size > 0:
        splits.append({'paths': tar_paths, 'size': tar_size, 'id': split_id})
    return splits
