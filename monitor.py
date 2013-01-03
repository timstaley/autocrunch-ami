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

logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s',
                        level=logging.INFO)

logger = logging.getLogger()
#ami.logger.setLevel(logging.WARN)
#drivecasa.logger.setLevel(logging.WARN)
#log_stdout = logging.StreamHandler(sys.stdout)
#log_stdout.setLevel(logging.INFO)
#logger.addHandler(log_stdout)

watchdir = '/home/ts3e11/test/data'
default_output_dir = os.path.expanduser("/home/ts3e11/ami_results")
default_ami_dir = "/data1/ami"
default_casa_dir = os.path.expanduser('/opt/soft/builds/casapy-33.0.16856-002-64b')
nthreads = 4


class MyEventHandler(pyinotify.ProcessEvent):
    def my_init(self, nthreads):
        self.active_new_files = {}
        self.pool = multiprocessing.Pool(nthreads)
        self.q = deque()

    def process_IN_CREATE(self, event):
        logger.debug("File transfer started: %s\n", event.pathname)
        self.active_new_files[event.pathname] = None

    def process_IN_CLOSE_WRITE(self, event):
        if event.pathname in self.active_new_files:
            logger.debug('New file transfer finished: %s\n' %
                                    event.pathname)
            self.active_new_files.pop(event.pathname)
            logger.info('Sending for processing: %s', event.pathname)
#            summary = process_rawfile(event.pathname)
#            processed_callback(summary)
            self.pool.apply_async(process_rawfile,
                                  [event.pathname],
                                  callback=processed_callback)

        else:
            logger.debug("Old file modified: %s\n" % event.pathname)


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

#notifier.loop()
logger.info("Watching %s ...", watchdir)
#Check / Process loop
while True:
#    logging.debug("Waiting...")
    notifier.process_events()
    while notifier.check_events(timeout=0.1):  #loop in case more events appear while we are processing
        notifier.read_events()
        notifier.process_events()
#    while len(handler.q) > handler.pool._processes:
#        job = handler.q.popleft()
#        result = job.get(timeout=600)
#        logger.info(result)
##    logging.debug("Sleeping...")
    time.sleep(0.5)


