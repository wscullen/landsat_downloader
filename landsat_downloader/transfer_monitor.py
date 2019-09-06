import os
import time
from threading import Thread
import queue
import sys
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class TransferMonitor:
    def __init__(self, filename, id):
        self.filename = filename
        self.start_time = datetime.now()
        self.finish_time = None
        self.stop = False
        self.id = id

        self.thread = Thread(target = self.file_size_monit, args = ( ))
        self.thread.start()


    def file_size_monit(self):
        file_size = 0
        stablized_size = 30 # wait 30 seconds before declaring file stable
        stable_count = 0
        time_count = 1
        step_size = 30

        while True:
            new_file_size = self.get_file_size()
            humanbytes =  self.convert_bytes(float(new_file_size))
            avg_speed = self.convert_bytes(float(new_file_size), raw='MB') / time_count

            logger.debug('{} ====> {} total transfer: {}, transfer speed {} MB/s'.format(self.id, self.filename, humanbytes, avg_speed))
            print('{} ====> {} total transfer: {}, transfer speed {} MB/s'.format(self.id, self.filename, humanbytes, avg_speed))

            if self.stop:
                break

            file_size = new_file_size
            time_count += step_size
            time.sleep(step_size)


    def get_file_size(self, human=False):

        if os.path.isfile(self.filename):
            file_info = os.stat(self.filename)
            if human:
                return self.convert_bytes(file_info.st_size)
            else:
                return file_info.st_size
        else:
            return -1

    def convert_bytes(self, num, raw=None):
        """
        this function will convert bytes to MB.... GB... etc
        """
        if raw:
            for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
                if raw == x:
                    return num
                num /= 1024.0

        for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
            if num < 1024.0:
                return "%3.1f %s" % (num, x)
            num /= 1024.0

    def finish(self):
        self.stop = True
        self.finish_time = datetime.now()

        self.time_delta = self.finish_time - self.start_time
        print('Time taken: total {}s, {} hours {} minutes {} seconds'
                .format(self.time_delta.total_seconds(),
                self.time_delta.seconds / (60 * 60),
                self.time_delta.seconds  % (60 * 60) / 60,
                self.time_delta.seconds % (60 * 60 * 60)))
        self.thread.join()
