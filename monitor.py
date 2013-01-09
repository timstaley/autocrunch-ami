#!/usr/bin/python
import pyinotify
import sys
import multiprocessing
import logging
import logging.handlers
import time
import os
import optparse
from functools import partial
import ami
import drivecasa

logger = logging.getLogger()

class options():
    """Dummy class serving as a placeholder for optparse handling."""
#    ami = "/opt/ami"
    ami = "/home/djt/ami"
    casa = '/opt/soft/builds/casapy-active'
    output_dir = "/home/ts3e11/ami_results"
    log_dir = '/tmp/autocruncher'
    nthreads = 6


def is_rawfile(filename):
    """Predicate function for identifying incoming AMI data"""
    if '.raw' in filename:
        return True
    return False

def process_rawfile(filename, ami_dir, casa_dir, output_dir):
    rawfile = os.path.basename(filename)
    reduce = ami.Reduce(ami_dir)
    groupname = rawfile.split('-')[0]
    group_dir = os.path.join(output_dir, groupname)
    ami_output_dir = os.path.join(group_dir, 'ami')
    try:
        obs_info = ami.process_rawfile(rawfile, ami_output_dir, reduce)
        drivecasa.process_observation(obs_info, group_dir, casa_dir)
    except (IOError, ValueError) as e:
        error_message = ("Hit exception reducing file: %s, exception reads:\n%s\n"
                         % (rawfile, e))
        return error_message

    return "Successfully processed " + filename

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


def main(options):
    watchdir = os.path.join(options.ami, 'LA/data')

    wm = pyinotify.WatchManager()
    pool = multiprocessing.Pool(options.nthreads)

    def asynchronously_process_rawfile(file_path, mp_pool):
#    summary = process_rawfile(file_path)
#    processed_callback(summary)
        mp_pool.apply_async(process_rawfile,
              [file_path, options.ami, options.casa, options.output_dir],
              callback=processed_callback)

    bound_asyncprocessor = partial(asynchronously_process_rawfile, mp_pool=pool)
    handler = RsyncNewFileHandler(nthreads=options.nthreads,
                                  file_predicate=is_rawfile,
                                  file_processor=bound_asyncprocessor)

    notifier = pyinotify.Notifier(wm, handler)

    wm.add_watch(watchdir, handler.mask, rec=True)

    logger.info("***********")
    logger.info('Watching %s', watchdir)
    logger.info('Ami dir %s', options.ami)
    logger.info('Casa dir %s', options.casa)
    logger.info('Ouput dir %s', options.output_dir)
    logger.info('Log dir %s', options.log_dir)
    logger.info("***********")
    notifier.loop()
    return 0

if __name__ == '__main__':
    if not os.path.isdir(options.log_dir):
        os.makedirs(options.log_dir)
    log_filename = os.path.join(options.log_dir, 'autocruncher_log')
    std_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    debug_formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')

    fhandler = logging.handlers.RotatingFileHandler(log_filename,
                            maxBytes=5e5, backupCount=10)
    fhandler.setFormatter(std_formatter)
    fhandler.setLevel(logging.INFO)
    dhandler = logging.handlers.RotatingFileHandler(log_filename + '.debug',
                            maxBytes=5e5, backupCount=10)
    dhandler.setFormatter(debug_formatter)
    dhandler.setLevel(logging.DEBUG)

    shandler = logging.StreamHandler()
    shandler.setFormatter(std_formatter)
    shandler.setLevel(logging.INFO)

    logger = logging.getLogger()
    logger.addHandler(fhandler)
    logger.addHandler(shandler)
    logger.addHandler(dhandler)

    sys.exit(main(options))
