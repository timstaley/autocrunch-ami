import driveami
import driveami.keys as amikeys
import drivecasa
import os
import logging

logger = logging.getLogger(__name__)

ami_clean_args = {
          "spw": '0:0~5',
          "imsize": [512, 512],
          "cell": ['5.0arcsec'],
          "pbcor": False,
#           "weighting": 'natural',
             "weighting": 'briggs',
             "robust": 0.5,
#          "weighting":'uniform',
          "psfmode": 'clark',
          "imagermode": 'csclean',
          }

def ami_rawfile_quicklook(filename, ami_dir, casa_dir, output_dir):
    """A data reduction subroutine (specific to user's application)."""
    rawfile = os.path.basename(filename)
    reduce = driveami.Reduce(ami_dir)
    groupname = rawfile.split('-')[0]
    group_dir = os.path.join(output_dir, groupname)
    group_ami_outdir = os.path.join(group_dir, 'ami')
    group_casa_outdir = os.path.join(group_dir, 'casa')
    group_fits_outdir = os.path.join(group_dir, 'images')
    try:
        obs_info = driveami.process_rawfile(rawfile, group_ami_outdir, reduce)
        image_casa_outdir = os.path.join(group_casa_outdir,
                                         obs_info[amikeys.obs_name])
        casa_logfile = os.path.join(group_casa_outdir, 'casalog.txt')
        casa_script = []
        vis = drivecasa.commands.import_uvfits(casa_script,
                                 uvfits_path=obs_info[amikeys.target_uvfits],
                                 out_dir=image_casa_outdir,
                                 overwrite=True)
        dirty_maps = drivecasa.commands.clean(casa_script,
                 vis_path=vis,
                 niter=200,
                 threshold_in_jy=2.5 * obs_info[amikeys.est_noise_jy],
                 mask='',
                 other_clean_args=ami_clean_args,
                 out_dir=image_casa_outdir,
                 overwrite=True)
        dirty_maps_fits_image = drivecasa.commands.export_fits(casa_script,
                                image_path=dirty_maps.image,
                                out_dir=group_fits_outdir,
                                overwrite=True)
        casa = drivecasa.Casapy(casa_logfile,
                                casa_dir)
        casa.run_script(casa_script, raise_on_severe=True)

    except Exception as e:
        error_message = ("Hit exception reducing file: %s, exception reads:\n%s\n"
                         % (rawfile, e))
        logger.exception(e)
        return error_message

    return "Successfully processed " + filename
