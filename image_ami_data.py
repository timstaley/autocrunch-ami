import os
import logging
import sys
import multiprocessing

import ami
import drivecasa

test_groups = {
       "test_crunch1": {
            "files": [
                "SWIFT_540255-121207.raw",
                "SWIFT_540255-121209.raw"
            ]
        }
    }

default_output_dir = os.path.expanduser("~/ami_results")
default_ami_dir = "/data1/ami"
default_casa_dir = os.path.expanduser('/opt/soft/builds/casapy-33.0.16856-002-64b')

def process_dataset(groups):
    print "Hello world"
    print "Processing ", groups
    listings = ami.process_data_groups(groups, default_output_dir, default_ami_dir)
    drivecasa.process_groups(listings, default_output_dir, default_casa_dir)
    return "Done"

def split_into_single_file_listings(groups_list):
    single_listings = []
    for grp in groups_list.keys():
        for file in groups_list[grp]['files']:
            single_listings.append({grp:{'files':[file]}})
    return single_listings


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
    single_file_listings = split_into_single_file_listings(test_groups)
    for listing in single_file_listings:
        result = pool.apply_async(process_dataset, [listing])
    print result.get(timeout=1200)
    print "Fin."


