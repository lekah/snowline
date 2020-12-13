import datetime
from matplotlib import pyplot as plt
import numpy as np
import tarfile, tempfile, json, os

from scipy.ndimage import measurements
from snowline.analysis.grid import Grid


PIXEL_SNOW = 1
PIXEL_UNKNOWN = 0
PIXEL_NOSNOW = -1


class SnowMap(object):
    _ARRAY_FILENAME = 'array.npy'
    _ATTRIBUTE_FILENAME = 'attributes.json'
    def __init__(self, array, is_internal=False):
        """
        :param array: the original array, containing values of -1 for no snow, 1 for snow,
        and 0 for unknown.
        :param bool is_internal: whether the array has been transformed to internal grid.
        """
        if type(array).__module__ != np.__name__:
            raise TypeError("array passed has to be numpy array")
        if not str(array.dtype).startswith('int'):
            raise TypeError("array passed has to be an integer array")
        if set(np.unique(array)).difference([PIXEL_SNOW, PIXEL_NOSNOW, PIXEL_UNKNOWN]):
            raise ValueError("Array can only containv values -1, 0, 1")
        self._array = array.copy()
        self._is_internal = is_internal
        # structure defines which neighborhood kind to apply.
        # For now Neumann, but maybe this can be an input #TODO
        self._structure = [[0,1,0], [1,1,1], [0,1,0]]

    @classmethod
    def from_netcdf(cls, filename, transform=True):
        """
        :param filename: a valid path to a netcdf file
        :param transform: whether to transform to internal coordinates.
        """
        from snowline.analysis.read_NetCDF import NetCDF4SnowMap
        netcdf = NetCDF4SnowMap(filename)
        array = netcdf.get_snowmap(transform=transform)
        return cls(array=array, is_internal=transform)
    @classmethod
    def load(cls, filename):
        """
        Given a filename, load the arrays and return a new instance of the class.
        The filename should ideally be created with the SnowMap.store method.
        If created by hand, it has to be a valid tar.gz compressed tar.
        """

        with tempfile.TemporaryDirectory() as temp_folder:
            with tarfile.open(filename, "r:gz", format=tarfile.PAX_FORMAT) as tar:
                tar.extractall(temp_folder)

            files_in_tar = set(os.listdir(temp_folder))
            if not cls._ATTRIBUTE_FILENAME in files_in_tar:
                raise OSError("Attributes file missing")
            if not cls._ARRAY_FILENAME in files_in_tar:
                raise OSError("Array file missing")

            with open(os.path.join(temp_folder, cls._ATTRIBUTE_FILENAME)) as f:
                attributes = json.load(f)
            array = np.load(os.path.join(temp_folder, cls._ARRAY_FILENAME))
            # TODO checks whether array is valid!
            new = cls(array, **attributes)
        return new

    def _get_attributes(self):
        """
        Utility function that returns all attributes to recreate an
        instance of this class
        """
        return {"is_internal":self._is_internal}

    def save(self, filename):
        """
        Saves the trajectory instance to tarfile.
        :param str filename: The filename. Won't be checked or modified with extension!
        """
        with tempfile.TemporaryDirectory() as temp_folder:
            np.save(os.path.join(temp_folder, self._ARRAY_FILENAME), self._array)
            with open(os.path.join(temp_folder, self._ATTRIBUTE_FILENAME), 'w') as f:
                json.dump(self._get_attributes(), f)            
            with tarfile.open(filename, "w:gz", format=tarfile.PAX_FORMAT) as tar:
                tar.add(temp_folder, arcname="")

    
    def copy(self):
        return self.__class__(array=self._array, **self._get_attributes())

    def get_array(self):
        return self._array.copy()

    def filter_size_nonsnow(self, limit_nonsnow_patch_size,
            include_unknown=True, verbose=False):
        """
        Cleans up snowmap by first finding patches (=clusters) of non-snow within snow and, second,
        getting rid of small non-snow clusters below the threshold size.
        :param int limit_nonsnow_patch_size: the threshold below which
                clusters of non-snow are removed.
        :param bool include_unknown: Treat unknown pixels as pixels not
                containing snow
        :param bool verbose: Allow for prints to stdout
        """

        if limit_nonsnow_patch_size < 1:
            return

        if include_unknown:
            msk = (self._array==PIXEL_NOSNOW) | (self._array==PIXEL_UNKNOWN)
        else:
            msk = (self._array==PIXEL_NOSNOW)
        # Use scipy measurements.label to find clusters.
        nonsnow_clusters, num_clusters = measurements.label(msk,
                structure=self._structure)
        cluster_indices, counts = np.unique(nonsnow_clusters, return_counts=True)
        msk_nonzero = cluster_indices != 0 # cluster 0 contains no snow, no need to include in count

        # remove all cluster that are smaller than limit
        small_cluster_indices = cluster_indices[counts < limit_nonsnow_patch_size]
        if verbose:
            print('   Reduced nonsnow clusters from {} to {}'.format(
                    num_clusters, num_clusters - len(small_cluster_indices)))
        msk = np.isin(nonsnow_clusters, small_cluster_indices)
        # msk is now set to True for all clusters that are smmaller limit_snow
        self._array[msk] = PIXEL_SNOW

    def filter_size_snow(self, limit_snow_patch_size,
            include_unknown=True, verbose=False):
        """
        Cleans up snowmap by finding patches (=clusters) of snow and
        removing clusters below the threshold size.
        :param int limit_snow_patch_size: the threshold cluster size
        :param bool include_unknown: Treat unknown pixels as pixels
                containing snow
        :param bool verbose: Allow for prints to stdout
        """
        if limit_snow_patch_size < 1:
            return
        if include_unknown:
            msk = (self._array==PIXEL_SNOW) | (self._array==PIXEL_UNKNOWN)
        else:
            msk = (self._array==PIXEL_SNOW)
        # Use scipy measurements.label to find clusters.
        # structure tells it which neighborhood kind to apply.
        # For now Neumann, but maybe this can be an input
        snow_clusters, num_clusters = measurements.label(msk,
                        structure=self._structure)
        cluster_indices, counts = np.unique(snow_clusters, return_counts=True)
        msk_nonzero = cluster_indices != 0 # cluster 0 contains no snow, no need to include in count
        small_cluster_indices = cluster_indices[counts < limit_snow_patch_size]
        if verbose:
            print('   Reduced snow clusters from {} to {}'.format(
                    num_clusters, num_clusters - len(small_cluster_indices)))
        msk = np.isin(snow_clusters, small_cluster_indices)
        self._array[msk] = PIXEL_NOSNOW

    def get_num_clusters(self):
        return measurements.label(self._array==PIXEL_SNOW,
                structure=self.  _structure)[1]

    def get_boundaries(self, transform=False):
        """
        Get the points around the snow patches
        :param transform: transform to WGS coordinates based on internal grid
        """
        # pad 0 around
        array = np.concatenate([np.zeros((1, self._array.shape[1])), self._array, 
                np.zeros((1, self._array.shape[1]))], axis=0)
        array = np.concatenate([np.zeros((array.shape[0], 1)), array, np.zeros((array.shape[0], 1))], axis=1)
        # TODO option to treat unknown as having snow?
        snow_clusters, num_clusters = measurements.label(array==PIXEL_SNOW,
                            structure=self._structure)
        cluster_indices, counts = np.unique(snow_clusters, return_counts=True)
        msk_nonzero = cluster_indices != 0 # cluster 0 contains no snow, no need to include in count
        if transform:
            grid = Grid()
        for cluster_index in cluster_indices[msk_nonzero]:
            cs  = plt.contour(snow_clusters==cluster_index, levels=(0.5,))
            boundaries_this_cluster = [(seg-1) for seg in cs.allsegs[0]]
            if transform:
                yield grid.transform_boundary(boundaries_this_cluster)
            else:
                yield boundaries_this_cluster



class UpdatedSnowMap(SnowMap):
    """
    A subclass of SnowMap, whole instances can be update with
    simple rules
    """
    def __init__(self, *args, **kwargs):
        # Make a way to pass a datetime. JSON Compatible!
        self._timestamp = kwargs.pop('timestamp', None)
        super().__init__(*args, **kwargs)

    def update(self, other, timestamp=None):
        """
        Will update the internal snowmap given the data in the input snowmap.
        Rules are (pixelwise):
          * if new map indicates snow, change to snow
          * if new map indicates no snow, change to no snow
          * if new map indicates unknown, do not change
        :param other: a valid snowmap
        :param timestamp: A timestamp of other
        """
        # first some checks on the snowmap:
        if not isinstance(other, SnowMap):
            raise TypeError("Expecting an instance of SnowMap")
        if self._is_internal != other._is_internal:
            raise ValueError("incompatible grids")
        if not(other._is_internal) and not(self._is_internal):
            print("WARNING: updating grids that might not be compatible")
        array = other.get_array()
        self._array[array==PIXEL_SNOW] = PIXEL_SNOW
        self._array[array == PIXEL_NOSNOW] = PIXEL_NOSNOW
        self._timestamp = timestamp

    def get_timestamp(self):
        return self._timestamp

    def is_newer(self, timestamp):
        """
        Given a timestamp, return True if this timestamp is newer (or if
        own timestamp is Nonw
        """
        return self._timestamp is None or self._timestamp < timestamp

    def _get_attributes(self):
        attrs = super()._get_attributes()
        attrs.update({'timestamp':self._timestamp})
        return attrs
