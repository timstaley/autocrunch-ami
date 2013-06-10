import ami
import drivecasa
import os


def process_rawfile(filename, ami_dir, casa_dir, output_dir):
    """A data reduction subroutine (specific to user's application)."""
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
