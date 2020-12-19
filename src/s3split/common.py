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


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)


def percent(val, tot):
    return round((val / tot) * 100, 1)


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


def count_path_depth(path):
    return len(path.strip('/').split('/'))


def split_file_by_size(path, max_size):
    LOGGER = get_logger()
    base_path = path
    base_depth = count_path_depth(path)
    LOGGER.info(f"path: {path}, base depth: {base_depth}")
    splits = []
    split_id = 1
    split_size = 0
    split_paths = []

    def _next_split():
        nonlocal splits, split_id, split_paths, split_size
        splits.append({'paths': split_paths, 'size': split_size, 'id': split_id})
        split_size = 0
        split_paths = []
        split_id += 1

    for dirpath, dirnames, filenames in os.walk(path):
        depth = count_path_depth(dirpath) - base_depth
        # LOGGER.info(f"path: {dirpath}, dirname:{dirnames}, filenames:{filenames}, depth: {depth}")
        for file in filenames:
            size = os.path.getsize(os.path.join(dirpath, file))
            if size + split_size > max_size:
                # LOGGER.info("=== SPLIT SIZE")
                _next_split()
            split_paths.append(os.path.relpath(os.path.join(dirpath, file), path))
            split_size += size
    _next_split()
    from pprint import pformat
    # LOGGER.info(pformat(splits))
    return splits


def split_searh_file(splits, prefix=None):
    ids = set()
    if prefix is None:
        for split in splits:
            ids.add(split.get('id'))
    else:
        for split in splits:
            for path in split.get('paths'):
                if prefix.strip('/') in path.strip('/'):
                    ids.add(split.get('id'))
    return list(ids)
    # list(dict.fromkeys(ids))


def split_get_dirs(splits):
    folders = set()
    for split in splits:
        for path in split.get('paths'):
            base = os.path.dirname(path)
            if base is not None and len(base) > 0:
                folders.add(base)
    return list(folders)

# def split_file_by_size(path, max_size):
#     """split first deep level objects (files or folder) in different tars with a maximum size)"""
#     if not os.path.isdir(path):
#         raise ValueError("path argument is not a valid directory")
#     splits = []
#     tar_size = 0
#     tar_paths = []
#     list_obj = [os.path.join(path, f) for f in os.listdir(path)]
#     random.shuffle(list_obj)
#     split_id = 1
#     for obj in list_obj:
#         size = get_path_size(obj)
#         # logger.debug(f"path: {p} - size: {size} - {tar_size + size} - {max_size * 1024 * 1024}")
#         if size > (max_size * 1024 * 1024):
#             raise SystemExit(f"Single path '{obj}' has a size bigger then max allowed split size. Exit")
#         if (tar_size + size) <= (max_size * 1024 * 1024):
#             tar_size += size
#             tar_paths.append(obj)
#         else:
#             splits.append({'paths': tar_paths, 'size': tar_size, 'id': split_id})
#             split_id += 1
#             tar_size = size
#             tar_paths = [obj]
#     if tar_size > 0:
#         splits.append({'paths': tar_paths, 'size': tar_size, 'id': split_id})
#     return splits


# def add_to_split(splits, file, max_size):
#     if not os.path.isfile(file):
#         raise ValueError("Only file can be added to tar")
#     split = splits[-1]
#     if split.get('size') + os.path.getsize(file) > max_size:
#         splits.append({'paths': [file], 'size': os.path.getsize(file), 'id': split.get('id') + 1})
#     else:
#         LOGGER = get_logger()
#         LOGGER.info(f"========> {splits}")
#         split = splits.pop()
#         splits.append({'paths': split.get('paths').append(file), 'size': split.get('size') + os.path.getsize(file), 'id': split.get('id')})
#     return splits


# def add_to_split_empty(splits=None):
#     if splits is None or len(splits) == 0:
#         return [{'paths': [], 'size': 0, 'id': 1}]
#     else:
#         split = splits[-1]
#         return splits.append({'paths': [], 'size': 0, 'id': split.get('id') + 1})