

import numpy as np, os
from snowline.analysis.snowmap import SnowMap, UpdatedSnowMap
from snowline.analysis.grid import Grid
from snowline.utils.time_utils import get_datetime_from_filename
from snowline.utils.s3_io import SnowlineDB, SatelliteDB, boundaries_to_geo


class SnowMapUpdater(object):
    def __init__(self, update_map_path=None, allow_blank=True,
            aws_access_key_id=None, aws_secret_access_key=None,
            verbose=True):
        self._aws_dict = dict(aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key)

        self._verbose = bool(verbose)
        try:
            if self._verbose:
                print("Reading state map from {}... ".format(update_map_path),
                        end="")
            if update_map_path is None:
                raise OSError("None is not a valid path")
            self._usm = UpdatedSnowMap.load(update_map_path)
            if self._verbose:
                print("Done")
        except OSError as e:
            if allow_blank:
                if self._verbose:
                    print("Failed, initializing with zeros")
                grid = Grid()
                self._usm = UpdatedSnowMap(array=Grid().zeros(), is_internal=True)
            else:
                if self._verbose:
                    print("Received exception: {}".format(e))
                raise e
        self._netcdf_file_list = []
        self._updated = False
        self._boundaries = None

    def set_netcdf_files(self, *args):
        """
        Sets manually netcdf_files to use and forces the updater to use
        those, regardless of timestamp etc.
        """
        for netcdf_file_path in args:
            if not netcdf_file_path.endswith('.nc'):
                raise ValueError("{} has wrong ending".format(netcdf_file_path))
            if not os.path.exists(netcdf_file_path):
                raise OSError("{} does not exist".format(netcdf_file_path))
            dt = get_datetime_from_filename(netcdf_file_path) # get_datetime_from_filename returns datetime
            self._netcdf_file_list.append((dt.timestamp(), netcdf_file_path))

    def get_netcdf_files(self, cache,
                sattelite_bucketname='snowlines-satellite'):
        """
        Searches the S3 for files and selects the ones that should be used,
        based on the timestamp. Downloads these to the cache if not
        already present
        TODO: allow for upper time limit
        """
        if not os.path.isdir(cache):
            raise OSError("Cache ({}) is not a directory".format(cache))
        if self._verbose:
            print("Seaching in {} for files".format(sattelite_bucketname))
        satellite = SatelliteDB(dbbucketname=sattelite_bucketname,
                **self._aws_dict)
        files_in_bucket = satellite.get_files()
        chosen_files = []
        for netcdf_file in files_in_bucket:
            timestamp = get_datetime_from_filename(netcdf_file).timestamp()
            # if timestamp > max_timestamp: continue
            if self._usm.is_newer(timestamp):
                chosen_files.append((timestamp, netcdf_file))
        if self._verbose:
            print("Found {} files, out of which {} are chosen based on"
                " timestamp".format(len(files_in_bucket), len(chosen_files)))
        if not chosen_files:
            # No files chosen means no download to be done
            return
        # Satellite instance downloads files here.
        # Will raise some boto3 error if htis fails, which should be
        # caught TODO
        satellite.download_files(list(zip(*chosen_files))[1], cache,
                overwrite=False)
        for timestamp, filename in chosen_files:
            self._netcdf_file_list.append((timestamp,
                    os.path.join(cache, filename)))

    def update(self, store=None):
        """
        Update the SnowMap
        """
        if len(self._netcdf_file_list) == 0:
            if self._verbose:
                print("Nothing to do, no new NetCDF files")
            return
        for timestamp, netcdf_file_path in sorted(self._netcdf_file_list):
            if self._verbose:
                print("Reading NetCDF file {}... ".format(netcdf_file_path),end="")
            snowmap = SnowMap.from_netcdf(netcdf_file_path, transform=True)
            array = snowmap.get_array()
            if self._verbose:
                print("Done, obtained array of shape {} x {}\n"
                "Distribution of pixel values is:".format(*array.shape))
            for unique, count in zip(*np.unique(array, return_counts=True)):
                if self._verbose:
                    print("  {:<2}: {}".format(unique, count))
            # Important, also update the timetamp.
            # could also be done for final, but leave for now for
            # reasons of stability
            self._usm.update(snowmap, timestamp=timestamp)
        self._updated = True # Flag to allow for calculation and upload
        # Problem might be if update doesnt run because no new files
        if self._verbose:
            print("Update complete, final distribution of values is:")
            for unique, count in zip(*np.unique(self._usm.get_array(),
                        return_counts=True)):
                print("  {:<2}: {}".format(unique, count))
        if store:
            # TODO checks for valid file paht and existing files!
            if self._verbose:
                print("Writing state map to {}".format(store))
            self._usm.save(store)

    def calculate_boundaries(self, size_filter_snow=0,
            size_filter_nonsnow=0):
        if not (self._updated):
            raise RuntimeError("Upload called without update having been"
                    " performed")

        if size_filter_snow and self._verbose:
            print("Reducing snow fields with parameter "
                "size_filter_snow={}".format(size_filter_snow))
        self._usm.filter_size_snow(size_filter_snow, verbose=self._verbose)
        if size_filter_nonsnow and self._verbose:
            print("Reducing non-snow fields with parameter "
                "size_filter_nonsnow={}".format(size_filter_nonsnow))
        self._usm.filter_size_nonsnow(size_filter_nonsnow,
                verbose=self._verbose)
        if self._verbose:
            print("Calculating state map boundaries")
        self._boundaries = list(self._usm.get_boundaries(transform=True))
        if self._verbose:
            print("Done")

    def upload(self, dry_run=False, wipe_previous=False):
        if self._boundaries is None:
            raise RuntimeError("Upload called without calculate_boundaries"
                " having been called")
        snowlinedb_kwargs = dict(dbbucketname='snowlines-database',
            snowlinebucketname='snowlines',
            dbname='snowline.json')
        snowlinedb_kwargs.update(self._aws_dict)
        # TODO: allow for user update of snowlinedb_kwargs
        sdb = SnowlineDB(**snowlinedb_kwargs)
        sdb.upload(self._boundaries, dry_run=dry_run,
                timestamp=self._usm.get_timestamp(), verbose=self._verbose,
                wipe_previous=wipe_previous)


def update_snowmap(state_map=None, new_state_map=None,
        netcdf_files=[], cache=None, allow_blank=True,
        aws_access_key_id=None, aws_secret_access_key=None,
        size_filter_snow=0, size_filter_nonsnow=0,
        dry_run=False, no_upload=False, no_boundaries=False,
        quiet=False, wipe_previous=False):
    """
    Takes as input the path to a instance of UpdateMap (if None creates one)
    and queries the DB for satellite images newer than this UpdateMap.
    Updates this Map with all newer images and stores this map in new_state_map, if given (otherwise will not store!)
    :param string state_map: The path to the update map
    :param string netcdf_file_path: The path to the NetCDF file
    :param string new_state_map: Optional, path to the new update map
    :param bool allow_blank: If set to True, allows to initialize
        an update map with zeros if state_map is None or invalid.
    :param aws_access_key_id: The id for aws if not stored in $HOME/.aws
    :param aws_secret_access_key: The key for aws if not stored in $HOME/.aws
    :param int size_filter_snow: Max pixel size of snow fields
    :param int size_filter_nonsnow: Max pixel size of nonsnow fields
    :param bool no_upload: Do not upload to DB
    :param bool dry_run: Perform a dry run, do not upload to AWS but
        put in temporary directory
    :param bool quiet: Quiet run, disable verbosity
    :param bool wipe_previous: Wipe the database from all previous data
        when uploading
    """
    smu = SnowMapUpdater(update_map_path=state_map,
            allow_blank=allow_blank, verbose=not(quiet),
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key)
    if netcdf_files:
        smu.set_netcdf_files(*netcdf_files) #TODO allow for multiples?
    else:
        if cache is None:
            raise ValueError("You need to provide a valid cache if "
                "you don't manually set netcdf_file_path")
        smu.get_netcdf_files(cache)
    smu.update(store=new_state_map)
    if no_boundaries:
        return
    smu.calculate_boundaries(size_filter_snow=size_filter_snow,
            size_filter_nonsnow=size_filter_nonsnow)
    if no_upload:
        return
    smu.upload(dry_run=dry_run, wipe_previous=wipe_previous)



if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('-s', '--state-map', help=('The path to the'
            ' state map to update'))
    parser.add_argument('-n', '--new-state-map', help=('Where to store '               'the new state map (if not provided will not store'))
    parser.add_argument('--netcdf-files', nargs='+', help=("Path to NetCDF "
            "files to be used for update. If none is provided will query"
            " on AWS and download"))
    parser.add_argument('-c', '--cache', help='A valid path to an existing '
            'directory that will be used as cache for netcdf files. '
            'Only needed if the option --netcdf-files is not provided')
    parser.add_argument('-b', '--allow-blank', action='store_true',
            help='If true, allows to start from a blank state map '
                'if there is no valid state path given')
    parser.add_argument('--size-filter-snow', type=int, default=0,
            help='Remove clusters of snow below this pixel size')
    parser.add_argument('--size-filter-nonsnow', type=int, default=0,
            help='Remove clusters of no snow with snow fields '
                    'below this pixel size')
    parser.add_argument('--no-upload', action='store_true',
            help='Disable the writing of boundary files and upload')
    parser.add_argument('--no-boundaries', action='store_true',
            help='Disable the boundary calculation and upload')
    parser.add_argument('-d', '--dry-run', action='store_true',
            help='Set this to make it a dry run and not upload to DB. '
                'Will write files to be uploaded to a temporary directory')
    parser.add_argument('-q', '--quiet', action='store_true',
            help='Perform a quiet run (no verbose output)')
    parser.add_argument('--wipe-previous', action='store_true',
            help='Wipes all previous data from the database, use with care')
    parser.add_argument('--aws-access-key-id', help="Access key id for"
            "upload, othewise will be read in ~/.aws by boto3")
    parser.add_argument('--aws-secret-access-key', help="Access key for"
            "upload, othewise will be read in ~/.aws by boto3")
    parsed = parser.parse_args()
    update_snowmap(**vars(parsed))
