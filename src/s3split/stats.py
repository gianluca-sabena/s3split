"""global stats"""
import os
import time
import threading
import traceback
import concurrent.futures
import tarfile
import tempfile
import s3split.s3util
import s3split.common
import s3split.common as com


class Stats():
    """Global stats ovject, updated from different working threads"""

    def __init__(self, interval, total_file, total_size):
        self._logger = s3split.common.get_logger()
        self._interval = interval
        self._total_file = total_file
        self._total_size = total_size
        self._stats = {}
        self._byte_sent = 0
        self._update_count = 0
        self._time_start = time.time()
        self._time_update = 0
        self._time_print_stat = time.time()
        self._lock = threading.Lock()

    # def _add(self, file):
    #     with self._lock:
    #         self._stats[file] = {'size': float(os.path.getsize(file)), 'transferred': 0, 'completed': False}

    def print(self):
        """print stats with logger"""
        completed = 0
        started = 0
        elapsed_time = round(time.time() - self._time_start, 1)
        msg = ""
        for file in self._stats:
            started += 1
            stat = self._stats[file]
            if stat['completed'] is True:
                completed += 1
            else:
                msg += f" - {file} ({com.percent(stat['transferred'], stat['size'])}%)\n"
        txt = (f"\n --- stats ---\nElapsed time: {elapsed_time} seconds\n"
               f"Data sent: {com.sizeof_fmt(self._byte_sent)} of {com.sizeof_fmt(self._total_size)} ({com.percent(self._byte_sent, self._total_size)}%)\n"
               f"Data processing rate: {com.sizeof_fmt((self._byte_sent)/elapsed_time)}\n"
               f"File completed: {completed} of {self._total_file} ({com.percent(completed, self._total_file)}%)")
        if len(msg) > 0:
            txt += f"\nFile(s) in progress:\n{msg}"
        self._logger.info(txt)

    def update(self, file, byte, total_size):
        """update byte sent for a file"""
        with self._lock:
            # add new file
            if self._stats.get(file) is None:
                self._stats[file] = {'size': total_size, 'transferred': 0, 'completed': False}
            self._stats[file]['transferred'] += byte
            self._byte_sent += byte
            if self._stats[file]['transferred'] == self._stats[file]['size']:
                self._stats[file]['completed'] = True
            self._update_count += 1
            if time.time() - self._time_print_stat > self._interval:
                self._time_print_stat = time.time()
                self.print()
