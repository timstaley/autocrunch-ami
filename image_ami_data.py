#!/usr/bin/env python
"""An integration test to ensure that the ami_quicklook routine is working"""
import os
import logging
import sys
import multiprocessing
from autocrunch import ami_rawfile_quicklook

logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(message)s',
                        level=logging.DEBUG)

default_output_dir = os.path.expanduser("/home/ts3e11/ami_test")
default_ami_dir = '/data2/ami'
default_casa_dir = None
# default_casa_dir = '/opt/soft/builds/casapy-active'
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
    # pool = multiprocessing.Pool(2)
#    process_dataset(test_groups)
#    result = pool.apply_async(process_dataset, [test_groups])
    return_message = ami_rawfile_quicklook('DEL2013-130927.raw',
                    default_ami_dir,
                    default_casa_dir,
                    default_output_dir)
    print return_message
#    for listing in single_file_listings:
#        result = pool.apply_async(process_dataset, [listing])
#    print result.get(timeout=1200)
    print "Fin."


