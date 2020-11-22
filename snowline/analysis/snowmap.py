from matplotlib import pyplot as plt
import numpy as np
from scipy.ndimage import measurements

PIXEL_SNOW = 1
PIXEL_UNKNOWN = 0
PIXEL_NOSNOW = -1


class SnowMap(object):
    def __init__(self, array):
        """
        :param array: the original array, containing values of 0 for no snow, 1 for snow,
        and -1 for unknown.
        """
        if type(array).__module__ != np.__name__:
            raise TypeError("array passed has to be numpy array")
        if array.dtype != 'int64':
            raise TypeError("array passed has to be an integer array")
        if set(np.unique(array)).difference([PIXEL_SNOW, PIXEL_NOSNOW, PIXEL_UNKNOWN]):
            raise ValueError("Array can only containv values -1, 0, 1")
        self._array = array.copy()
        # structure defines which neighborhood kind to apply.
        # For now Neumann, but maybe this can be an input #TODO
        self._structure = [[0,1,0], [1,1,1], [0,1,0]]

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

    def get_boundaries(self):
        """
        Get the points around the snow patches
        """
        # pad 0 around
        array = np.concatenate([np.zeros((1, self._array.shape[1])), self._array, 
                np.zeros((1, self._array.shape[1]))], axis=0)
        array = np.concatenate([np.zeros((array.shape[0], 1)), array, np.zeros((array.shape[0], 1))], axis=1)

        snow_clusters, num_clusters = measurements.label(array==PIXEL_SNOW,
                            structure=self._structure)
        cluster_indices, counts = np.unique(snow_clusters, return_counts=True)
        msk_nonzero = cluster_indices != 0 # cluster 0 contains no snow, no need to include in count
        for cluster_index in cluster_indices[msk_nonzero]:
            cs  = plt.contour(snow_clusters==cluster_index, levels=(0.5,))
            yield [(seg-1) for seg in cs.allsegs[0]]
        # Legacy code to make the boundary, which might be useful later
        # ~ boundary_msk = get_boundary_msk(snow_clusters, 1)
        # ~ R = np.array([[0,-1],[1,0]]) # Rotation matrix for 90 degress

        # ~ points_all = []

        # ~ Npixels = 2*boundary_msk.sum() # every pixel can be visited 2, in theory
        # ~ remainder = boundary_msk.copy()
        # ~ count = 0
        # ~ i, j = get_leftmost(remainder)
        # ~ start_i, start_j = i, j
        # ~ v = np.array([1,0])
        # ~ points = []
        # ~ while True:
            # ~ v = -v # rotating v around 180
            # ~ for t in range(4):  # applying 4 rotation (leftwise, consecutively)
                # ~ v = R.dot(v)
                # ~ if boundary_msk[i+v[0], j+v[1]]: # if I find a boundary pixel, I go there
                    # ~ break
                # ~ else:
                    # ~ # If I don't find a boundary pixel, this is the boundary.
                    # ~ # add a point.
                    # ~ points.append([j+0.5*v[1], i+0.5*v[0]])
                    # ~ #points.append([ i+0.5*v[0], j+0.5*v[1]])
            # ~ last_i, last_j = i, j
            # ~ remainder[i, j] = False
            # ~ i+=v[0]
            # ~ j+=v[1]
            # ~ count+=1
            # ~ if (i==start_i and j==start_j):
                # ~ # close the loop
                # ~ points.append(points[0])
                # ~ break
            # ~ if count > Npixels:
                # ~ raise RuntimeError("Number of iterations exceeded number of boundary pixels")

