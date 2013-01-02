import pyinotify
import sys
import multiprocessing
import logging
import time
from collections import deque

watchdir = '/home/ts3e11/test/data'

class MyEventHandler(pyinotify.ProcessEvent):
    def my_init(self, nthreads):
        self.active_new_files = {}
        self.pool = multiprocessing.Pool(nthreads)
        self.q = deque()

    def process_IN_CREATE(self, event):
        logging.debug("File transfer started: %s\n", event.pathname)
        self.active_new_files[event.pathname] = None

    def process_IN_CLOSE_WRITE(self, event):
        if event.pathname in self.active_new_files:
            logging.debug('New file transfer finished: %s\n' %
                                    event.pathname)
            self.active_new_files.pop(event.pathname)
            logging.info('Sending for processing: %s', event.pathname)
            self.q.append(self.pool.apply_async(process_rawfile,
                                                [event.pathname]))

        else:
            logging.debug("Old file modified: %s\n" % event.pathname)

def process_rawfile(filename):
    print "****** Processing ******"
    grp_name = filename.split('-')[0]
    raw_listing = {grp_name: {'files':filename}}
    logging.info("Simulating processing of %s ", filename)
    logging.info("(Grouped under %s)", grp_name)
    time.sleep(1)
    return "****** " + filename + " done"

logging.basicConfig(format='%(levelname)s:%(message)s',
                    level=logging.DEBUG)

wm = pyinotify.WatchManager()
handler = MyEventHandler(nthreads=2)
notifier = pyinotify.Notifier(wm, handler)
mask = pyinotify.IN_CREATE | pyinotify.IN_CLOSE_WRITE
wm.add_watch(watchdir, mask, rec=True)

#notifier.loop()

#Check / Process loop
while True:
#    logging.debug("Waiting...")
    notifier.process_events()
    while notifier.check_events(timeout=0.1):  #loop in case more events appear while we are processing
        notifier.read_events()
        notifier.process_events()
    while len(handler.q):
        job = handler.q.popleft()
        result = job.get(timeout=2)
        logging.info(result)
#    logging.debug("Sleeping...")
    time.sleep(0.5)


