from snowline.analysis.snowmap import SnowMap, UpdatedSnowMap
from snowline.analysis.grid import Grid
from snowline.utils.s3_io import SnowlineDB, boundaries_to_geo




def update_snowmap(update_map_path, netcdf_file_path,
        new_update_map_path=None, allow_start_zeros=False,
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
        usm = UpdatedSnowMap.load(update_map_path)
    except OSError as e:
        if allow_start_zeros:
            grid = Grid()
            usm = UpdatedSnowMap(array=Grid().zeros(), s_internal=True)
        else:
            raise e
    snowmap = SnowMap.from_netcdf(filename, transform=False)
    # Updating the array
    usm.update(snowmap)

    # If wanted, store the array:
    if new_update_map_path is not None:
        usm.save(new_update_map_path)

    boundaries = list(usm.get_boundaries(transform=True))

    snowlinedb_kwargs = dict(dbbucketname='snowlines-database',
            snowlinebucketname='snowlines',
            dbname='snowline.json')
    # TODO: allow for user update of snowlinedb_kwargs
    sdb = SnowlineDB{**snowlinedb_kwargs)
    sdb.upload(boundaries, dry_run=dry_run)
    
