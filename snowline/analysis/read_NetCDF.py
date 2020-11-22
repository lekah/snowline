import netCDF4
import numpy as np
from snowline.analysis.snowmap import PIXEL_NOSNOW, PIXEL_SNOW, PIXEL_UNKNOWN
from snowline.analysis.grid import Grid

class NetCDF4SnowMap(object):
    _REQUIRED_VARS = ('lon', 'lat', 'IDEPIX_CLOUD', 
            'IDEPIX_SNOW_ICE')
    _OPTIONAL_VARS = ('IDEPIX_CLOUD_BUFFER', 'IDEPIX_INVALID')
    _KEY_DICT = {
            'IDEPIX_CLOUD':'cloud',
            'IDEPIX_SNOW_ICE':'snow',
            'IDEPIX_CLOUD_BUFFER':'cloud_buffer',
            'IDEPIX_INVALID':'invalid',
            'IDEPIX_LAND':'land',
    }
    _KEY_LON = 'lon'
    _KEY_LAT = 'lat'
    def __init__(self, filename):
        """
        :param filename: A valid netCDF4 filename
        """
        self._ncfile = netCDF4.Dataset(filename, mode='r', format='NETCDF4_CLASSIC')
        for varname in self._REQUIRED_VARS:
            if varname not in self._ncfile.variables.keys():
                raise ValueError("Missing variable {} in netCDF4 {}".format(
                        varname, filename))
        self._load_data()
    def _load_data(self):

        lat_data = self._ncfile.variables['lat'][:].data
        lon_data = self._ncfile.variables['lon'][:].data

        lat_argsort = lat_data.argsort()
        if (lat_argsort == range(len(lat_argsort))).all():
            invert_lat = False
            # latitudes are properly sorted
        elif (lat_argsort[::-1] == range(len(lat_argsort))).all():
            invert_lat = True
            lat_data = lat_data[::-1]
        else:
            raise NotImplemented("Coordinates in random order"
                        " have not been implemented")

        if not (lon_data.argsort() == range(len(lon_data))).all():
            # Doesn't seem to occur with NetCDF files 
            raise NotImplemented("Have not implemented sorting of longitude grid")

        self._vars = {self._KEY_LON:lon_data, self._KEY_LAT:lat_data}
        for key, newkey in self._KEY_DICT.items():
            try:
                # Loading just the negative mask that is everything we need:
                data = ~self._ncfile[key][:,:].mask.T
            except KeyError:
                if key in self._REQUIRED_VARS:
                    raise KeyError("Key {} not in NetCDF".format(key))
                else:
                    continue
            if invert_lat:
                data = data[::-1, :]
            self._vars[newkey] = data.T[::-1,::-1]


    def get_snowmap(self, transform=True):
        """
        Builds a valid snowmap with the values:
         - {} for a pixel definitively showing snow
         - {} for a pixel definitely showing no snow
         - {} for a pixel with uncertainty due to cloud cover or invalid
        
        :param bool transform: Transform the map to internal coordinates
        """.format(PIXEL_SNOW, PIXEL_NOSNOW, PIXEL_UNKNOWN)

        # creating a snowmap of right dimensions
        # WATCH OUT: small memory footprint achieved via np.int8 (-128 to 127), since only 3 values need
        # to be stored. Careful later when multiplying this matrix!
        snowmap = np.zeros((self._vars[self._KEY_LAT].shape[0], self._vars[self._KEY_LON].shape[0]),
                dtype=np.int8)
        # by default there is no snow anywhere
        snowmap[:,:] = PIXEL_NOSNOW

        # setting pixels to 0 that have to be classified with uncertainty
        for key in ('cloud', 'cloud_buffer', 'invalid'):
            try:
                snowmap[self._vars[key]] = PIXEL_UNKNOWN
            except KeyError:
                # So far not checking whether cloud_buffer and invalid are given.
                # so ignoring this error
                pass

        # setting pixels to 1 that definitely contain snow
        # some pixels that were with cloud_buffer will be overwritten
        snowmap[self._vars['snow']] = PIXEL_SNOW

        if transform:
            grid = Grid()
            return grid, grid.transform_map_from_grid(snowmap.T,
                    self._vars[self._KEY_LON], self._vars[self._KEY_LAT], fill_value=PIXEL_UNKNOWN).T
        else:
            return snowmap

