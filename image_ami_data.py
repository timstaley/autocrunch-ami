#!/usr/bin/env python

import os
import logging
import sys
import multiprocessing
import ami
import drivecasa
from autocrunch import process_rawfile

logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(message)s',
                        level=logging.DEBUG)

#watchdir = '/opt/ami/LA/data'
#default_ami_dir = '/opt/ami'    
#default_casa_dir = os.path.expanduser('/opt/soft/builds/casapy-34.0.19988-002-64b')
default_output_dir = os.path.expanduser("/home/ts3e11/ami_results")
default_ami_dir = '/data2/ami'
default_casa_dir = os.path.expanduser('/opt/soft/builds/casapy-33.0.16856-002-64b')
nthreads = 4

def processed_callback(summary):
    logger.info('*** Job complete: ' + summary)


if __name__ == "__main__":
    logging.basicConfig(format='%(name)s:%(message)s',
                        filemode='w',
                        filename="ami-crunch.log",
                        level=logging.DEBUG)
    log_stdout = logging.StreamHandler(sys.stdout)
    log_stdout.setLevel(logging.INFO)
    logger = logging.getLogger()
    logger.addHandler(log_stdout)
    pool = multiprocessing.Pool(2)
#    process_dataset(test_groups)
#    result = pool.apply_async(process_dataset, [test_groups])
    process_rawfile('SWIFT_541371-121212.raw',
                    default_ami_dir,
                    default_casa_dir,
                    default_output_dir)
#    for listing in single_file_listings:
#        result = pool.apply_async(process_dataset, [listing])
#    print result.get(timeout=1200)
    print "Fin."


