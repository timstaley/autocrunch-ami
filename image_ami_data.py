import os
import logging
import sys
import multiprocessing
import ami
import drivecasa

logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(message)s',
                        level=logging.DEBUG)

#watchdir = '/opt/ami/LA/data'
#default_ami_dir = '/opt/ami'    
#default_casa_dir = os.path.expanduser('/opt/soft/builds/casapy-34.0.19988-002-64b')
default_output_dir = os.path.expanduser("/home/ts3e11/ami_results")
default_ami_dir = '/data1/ami'
watchdir = '/data1/ami/LA/data'
default_casa_dir = os.path.expanduser('/opt/soft/builds/casapy-33.0.16856-002-64b')
nthreads = 4

def process_rawfile(filename):
    rawfile = os.path.basename(filename)
    reduce = ami.Reduce(default_ami_dir)
    groupname = rawfile.split('-')[0]
    group_dir = os.path.join(default_output_dir, groupname)
    ami_output_dir = os.path.join(group_dir, 'ami')
    try:
        obs_info = ami.process_rawfile(rawfile, ami_output_dir, reduce)
#        drivecasa.process_observation(obs_info, group_dir, default_casa_dir)
    except (IOError, ValueError) as e:
        error_message = ("Hit exception reducing file: %s, exception reads:\n%s"
                         % (rawfile, e.message))
        return error_message

    return ("Successfully processed %s \n Rain: %d \n"
            % (filename, obs_info[ami.keys.rain]))

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
    process_rawfile('SWIFT_541371-121212.raw')
#    for listing in single_file_listings:
#        result = pool.apply_async(process_dataset, [listing])
#    print result.get(timeout=1200)
    print "Fin."


