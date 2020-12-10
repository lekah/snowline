import numpy as np
from scipy.interpolate import RegularGridInterpolator



# 1d east corresponds to roughly 76km at our latitude
# 1d north is roughly 111 km at our latitude
WGS_DIST_X = 76e3
WGS_DIST_Y = 111e3

class Grid(object):
    # Using international WGS 84 coordinates. CH lies between 45.8 and 47.8N and between 5.9 and 10.5E 
    # Defining our grid here, hardcoded:
    LOWER_LEFT = ( 5.7, 45.7)
    UPPER_RIGHT = (10.7, 47.9)

    # The distance between neighboring gridpoint in meters
    # This could be changed by users in the future, for now I hard code here #TODO
    GRID_PREC = 300
    def __init__(self):
        self._origin = np.array(self.LOWER_LEFT)

        self._gridsize_x = int(np.round((self.UPPER_RIGHT[0] - self.LOWER_LEFT[0])*WGS_DIST_X / self.GRID_PREC))
        self._gridsize_y = int(np.round((self.UPPER_RIGHT[1] - self.LOWER_LEFT[1])*WGS_DIST_Y / self.GRID_PREC))

        self._transformation = (np.array(self.UPPER_RIGHT) - self._origin ) / np.array(
                [self._gridsize_x, self._gridsize_y])

        grid_x = np.linspace(self.LOWER_LEFT[0], self.UPPER_RIGHT[0], self._gridsize_x)
        grid_y = np.linspace(self.LOWER_LEFT[1], self.UPPER_RIGHT[1], self._gridsize_y)

        self._coords_mesh = np.array(np.meshgrid(grid_x, grid_y))

    def transform_map(self, map_, coords, fill_value):
        """
        Transforms a map in different coordinates (given by coords) to the internal grid.

        :param map_: A 2-D map
        :param coords: A list of 4 coordinates as returned by utils.io.get_coordinates,
            in the order upper left, lower left, upper right, lower right.
        :param fill_value: The value to use for out-of-bounds points
        """
        raise NotImplemented("Haven't correctly implemented X Y")
        # quick check to make sure coords are aligned as I expect
        for i, j, k, l in ((0,0,1,0), # Making sure first 2 vectors have same x coordinate (longitude).
                (2,0,3,0), (0,1,2,1), (1,1,3,1)): # Same for 3 additional checks
            assert coords[i][j] == coords[k][l], "Wrong coordinate alignment"

        grid_x_given = np.linspace(coords[1][0], coords[2][0], map_.shape[0])
        grid_y_given = np.linspace(coords[1][1], coords[2][1], map_.shape[1])
        return self.transform_map_from_grid(map_, grid_x_given, grid_y_given, fill_value)

    def transform_map_from_grid(self, map_, grid_x, grid_y, fill_value):
        rgi = RegularGridInterpolator((grid_x, grid_y), map_,
                        bounds_error=False, fill_value=fill_value)
        return rgi(self._coords_mesh.T, method='nearest').astype(map_.dtype)


    def transform_boundaries(self, boundaries):
        """
        Transform boundaries in internal grid, as returned by SnowMap.get_boundaries,
        to WGS coordinates.
        """
        for boundaries_this_cluster in boundaries:
            yield transform_boundary(boundaries_this_cluster)

    def transform_boundary(self, boundaries_cluster):
        """
        Transform a single boundary from internal grid
        to WGS coordinates.
        """
        tmp_list = []
        for boundary in boundaries_cluster:
            tmp_list.append(self._transformation*boundary + self._origin)
        return tmp_list

    def zeros(self, dtype=np.int8):
        """
        Convenience function, returns a grid of the right shape filled with zeros
        """
        return np.zeros((self._gridsize_y, self._gridsize_x), dtype=dtype)
