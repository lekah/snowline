from matplotlib import pyplot as plt
import numpy as np
from scipy.ndimage import measurements
from snowline.analysis.grid import Grid

PIXEL_SNOW = 1
PIXEL_UNKNOWN = 0
PIXEL_NOSNOW = -1


class SnowMap(object):
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

    def copy(self):
        return self.__class__(array=self._array, is_internal=self._is_internal)

    def get_array(self):
        return self._array.copy()

    def filter_size_nonsnow(self, limit_nonsnow_patch_size):
        """
        Cleans up snowmap by first finding patches (=clusters) of non-snow within snow and, second,
        getting rid of small non-snow clusters below the threshold size.
        :param int limit_nonsnow_patch_size: the threshold below which clusters of non-snow are
        removed.
        """

        if limit_nonsnow_patch_size < 1:
            return
    
        # Use scipy measurements.label to find clusters.

        nonsnow_clusters, num_clusters = measurements.label(self._array==PIXEL_NOSNOW, 
                                structure=self._structure)
        cluster_indices, counts = np.unique(nonsnow_clusters, return_counts=True)
        msk_nonzero = cluster_indices != 0 # cluster 0 contains no snow, no need to include in count

        # remove all cluster that are smaller than limit
        small_cluster_indices = cluster_indices[counts < limit_nonsnow_patch_size]
        msk = np.isin(nonsnow_clusters, small_cluster_indices)
        # msk is now set to True for all clusters that are smmaller limit_snow
        self._array[msk] = PIXEL_SNOW

    def filter_size_snow(self, limit_snow_patch_size):
        """
        Cleans up snowmap by finding patches (=clusters) of snow and
        removing clusters below the threshold size.
        :param int limit_snow_patch_size: the threshold cluster size
        """
        if limit_snow_patch_size < 1:
            return
        # Use scipy measurements.label to find clusters.
        # structure tells it which neighborhood kind to apply.
        # For now Neumann, but maybe this can be an input #TODO
        snow_clusters, num_clusters = measurements.label(self._array==PIXEL_SNOW, 
                        structure=self._structure)
        cluster_indices, counts = np.unique(snow_clusters, return_counts=True)
        msk_nonzero = cluster_indices != 0 # cluster 0 contains no snow, no need to include in count
        small_cluster_indices = cluster_indices[counts < limit_snow_patch_size]
        msk = np.isin(snow_clusters, small_cluster_indices)
        self._array[msk] = PIXEL_NOSNOW

    def get_num_clusters(self):
        return measurements.label(self._array==PIXEL_SNOW, structure=self._structure)[1]

    def get_boundaries(self, transform=False):
        """
        Get the points around the snow patches
        :param transform: transform to WGS coordinates based on internal grid
        """
        # pad 0 around
        array = np.concatenate([np.zeros((1, self._array.shape[1])), self._array, 
                np.zeros((1, self._array.shape[1]))], axis=0)
        array = np.concatenate([np.zeros((array.shape[0], 1)), array, np.zeros((array.shape[0], 1))], axis=1)

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
    def update(self, other):
        """
        Will update the internal snowmap given the data in the  input snowmap.
        Rules are (pixelwise):
          * if new map indicates snow, change to snow
          * if new map indicates no snow, change to no snow
          * if new map indicates unknown, do not change
        :param other: a valid snowmap
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
