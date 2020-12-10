import numpy as np

from snowline.analysis.snowmap import SnowMap, UpdatedSnowMap
from snowline.analysis.grid import Grid
from snowline.utils.s3_io import SnowlineDB, boundaries_to_geo


def update_snowmap(update_map_path, netcdf_file_path,
        new_update_map_path=None, allow_start_zeros=True,
        dry_run=False):
    """
    Takes as input the path to a valid stored instance of UpdateMap and
    the path to a valid NetCDF file, and updates the map.
    Stores this map in new_update_map_path, if given (otherwise will not store!)
    :param string update_map_path: The path to the update map
    :param string netcdf_file_path: The path to the NetCDF file
    :param string new_update_map_path: Optional, path to the new update map
    :param bool allow_start_zeros: If set to True, allows to initialize
        an update map with zeros if update_map_path is None or invalid.
    :param bool dry_run: Perform a dry run, do not upload to AWS
    """
    try:
        print("Reading state map from {}... ".format(update_map_path), end="")
        if update_map_path is None:
            raise OSError("None is not a valid path")
        usm = UpdatedSnowMap.load(update_map_path)
        print("Done")
    except OSError as e:
        if allow_start_zeros:
            print("Failed, initializing with zeros")
            grid = Grid()
            usm = UpdatedSnowMap(array=Grid().zeros(), is_internal=True)
        else:
            print("Received exception: {}".format(e))
            raise e
    print("Reading NetCDF file {}... ".format(netcdf_file_path),end="")
    snowmap = SnowMap.from_netcdf(netcdf_file_path, transform=True)
    array = snowmap.get_array()
    print("Done, obtained file of shape {} x {}\n"
            "Distribution of pixel values is:".format(*array.shape))
    for unique, count in zip(*np.unique(array, return_counts=True)):
        print("  {:<2}: {}".format(unique, count))

    print("Updating state map... ", end="")
    usm.update(snowmap)
    print("Done\n")
    # If wanted, store the array:
    if new_update_map_path is not None:
        print("Saving state map to {}".format(new_update_map_path))
        usm.save(new_update_map_path)

    print("Calculating state map boundaries")
    boundaries = list(usm.get_boundaries(transform=True))
    print("Done")
    snowlinedb_kwargs = dict(dbbucketname='snowlines-database',
            snowlinebucketname='snowlines',
            dbname='snowline.json')
    # TODO: allow for user update of snowlinedb_kwargs
    sdb = SnowlineDB(**snowlinedb_kwargs)
    sdb.upload(boundaries, dry_run=dry_run, halt_when_testing=False)


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('-u', '--update_map_path', help=('The path to the'
            'old state map'))
    parser.add_argument('-c', '--netcdf_file_path', help=("Path to NetCDF "
            "file to be used for update"), required=True)
    parser.add_argument('-n', '--new_update_map_path',
            help='Where to store the new state map (if not provided will not store')
    parser.add_argument('-z', '--allow_start_zeros', action='store_true',
        help='If true, allows to start from 0 if there is no update_map_path given')
    parser.add_argument('-d', '--dry-run', action='store_true',
            help='Set this to make it a dry run and not upload to DB')

    update_snowmap(**vars(parser.parse_args()))
