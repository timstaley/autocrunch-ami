#!/usr/bin/python
import pyinotify
import sys
import multiprocessing
import logging
import time
import os
from collections import deque

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

watchdir = '/opt/ami/LA/data'
default_output_dir = os.path.expanduser("/home/ts3e11/ami_results")
default_ami_dir = '/opt/ami'
default_casa_dir = os.path.expanduser('/opt/soft/builds/casapy-34.0.19988-002-64b')
nthreads = 4


class MyEventHandler(pyinotify.ProcessEvent):
    def my_init(self, nthreads):
        self.active_new_files = {}
        self.pool = multiprocessing.Pool(nthreads)
        self.q = deque()

    def process_IN_CREATE(self, event):
#	if os.path.splitext(event.pathname)[-1] == '.raw':
    	if '.raw' in event.pathname:
            logger.debug("RawFile transfer started: %s\n", event.pathname)
            self.active_new_files[event.pathname] = None

    def process_IN_CLOSE_WRITE(self, event):
        if event.pathname in self.active_new_files:
            logger.debug('New rawfile transfer finished: %s\n' %
                                    event.pathname)
            self.active_new_files.pop(event.pathname)
            logger.info('Sending for processing: %s', event.pathname)
            ## Good idea to  test called code in single threaded mode first,
            ## since exceptions do not propagate back from subprocesses.
            summary = process_rawfile(event.pathname)
            processed_callback(summary)
#            self.pool.apply_async(process_rawfile,
#                                  [event.pathname],
#                                  callback=processed_callback)

        else:
            logger.debug("File modified: %s\n" % event.pathname)


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
        error_message = ("Hit exception reducing file: %s, exception reads:\n%s"
                         % (rawfile, e.message))
        return error_message

    return "Successfully processed " + filename

def processed_callback(summary):
    logger.info('*** Job complete: ' + summary)

wm = pyinotify.WatchManager()
handler = MyEventHandler(nthreads=nthreads)
notifier = pyinotify.Notifier(wm, handler)
mask = pyinotify.IN_CREATE | pyinotify.IN_CLOSE_WRITE
wm.add_watch(watchdir, mask, rec=True)

logger.info("Watching %s ...", watchdir)
notifier.loop()

