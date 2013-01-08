#!/usr/bin/python
import pyinotify
import sys
import multiprocessing
import logging
import time
import os
from functools import partial

#logging.basicConfig(format='%(levelname)s:%(message)s',
#                    level=logging.INFO)

import ami
import drivecasa

logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(message)s',
                        level=logging.INFO)

logger = logging.getLogger()
#ami.logger.setLevel(logging.WARN)
#drivecasa.logger.setLevel(logging.WARN)
#log_stdout = logging.StreamHandler(sys.stdout)
#log_stdout.setLevel(logging.INFO)
#logger.addHandler(log_stdout)

watchdir = '/home/ts3e11/test/data'
default_output_dir = os.path.expanduser("/home/ts3e11/ami_results")
#default_ami_dir = '/opt/ami'
default_ami_dir = '/data1/ami'
#default_casa_dir = os.path.expanduser('/opt/soft/builds/casapy-34.0.19988-002-64b')
default_casa_dir = os.path.expanduser('/opt/soft/builds/casapy-33.0.16856-002-64b')
nthreads = 4


def is_probably_rawfile(filename):
    if '.raw' in filename:
        return True
    return False

def process_rawfile(filename):
    rawfile = os.path.basename(filename)
    reduce = ami.Reduce(default_ami_dir)
    groupname = rawfile.split('-')[0]
    group_dir = os.path.join(default_output_dir, groupname)
    ami_output_dir = os.path.join(group_dir, 'ami')
    try:
        obs_info = ami.process_rawfile(rawfile, ami_output_dir, reduce)
        drivecasa.process_observation(obs_info, group_dir, default_casa_dir)
    except (IOError, ValueError) as e:
        error_message = ("Hit exception reducing file: %s, exception reads:\n%s\n"
                         % (rawfile, e))
        return error_message

    return "Successfully processed " + filename

def asynchronously_process_rawfile(file_path, mp_pool):
#    summary = process_rawfile(file_path)
#    processed_callback(summary)
    mp_pool.apply_async(process_rawfile,
                          [file_path],
                          callback=processed_callback)



def processed_callback(summary):
    logger.info('*** Job complete: ' + summary)

class RsyncNewFileHandler(pyinotify.ProcessEvent):
    """Identifies new rsync'ed files and passes their path for processing.

    rsync creates temporary files with a `.` prefix and random 6 letter suffix,
    then renames these to the original filename when the transfer is complete.
    To reliably catch (only) new transfers while coping with this file-shuffling,
    we must do a little bit of tedious file tracking, using
    the internal dict `tempfiles`.
    Note we only track those files satisfying the condition
    ``file_predicate(basename)==True``.

    """
    def my_init(self, nthreads, file_predicate, file_processor):
        self.mask = pyinotify.IN_CREATE | pyinotify.IN_MOVED_TO
        self.tempfiles = {}
        self.predicate = file_predicate
        self.process = file_processor

    def process_IN_CREATE(self, event):
        original_filename = os.path.splitext(event.name[1:])[0]
        if self.predicate(original_filename):
            logger.debug("Transfer started, tempfile at:\n\t%s\n",
                         event.pathname)
            self.tempfiles[original_filename] = event.pathname

    def process_IN_MOVED_TO(self, event):
        if event.name in self.tempfiles:
            self.tempfiles.pop(event.name)
            logger.info('Sending for processing: %s', event.pathname)
            self.process(event.pathname)


wm = pyinotify.WatchManager()

pool = multiprocessing.Pool(4)
bound_processor = partial(asynchronously_process_rawfile, mp_pool=pool)

handler = RsyncNewFileHandler(nthreads=nthreads,
                              file_predicate=is_probably_rawfile,
                              file_processor=bound_processor)

notifier = pyinotify.Notifier(wm, handler)

wm.add_watch(watchdir, handler.mask, rec=True)

logger.info("Watching %s ...", watchdir)
notifier.loop()

