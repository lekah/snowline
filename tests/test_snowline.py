import unittest
import numpy as np
import os, tempfile
from scipy.ndimage import measurements

from snowline.analysis.snowmap import SnowMap, UpdatedSnowMap
from snowline.analysis.grid import Grid

class TestSnowmap(unittest.TestCase):
    def test_read_snowline_1(self):
        filename = os.path.join(
                os.path.abspath(os.path.dirname(__file__)), 
                'data',
'L2SNOW_reproj_idepix_subset_S3A_OL_1_EFR____20191214T093535_20191214T093835_20191215T142923_0179_052_307_2160_LN1_O_NT_002.SEN3.nc'
            )
        snowmap_ntf = SnowMap.from_netcdf(filename, transform=False)
        self.assertFalse(snowmap_ntf._is_internal)

        snowmap = SnowMap.from_netcdf(filename, transform=True)
        self.assertTrue(snowmap._is_internal)


        array_before = snowmap.get_array()
        # Checking whether null operations change the array
        snowmap.filter_size_nonsnow(0)
        snowmap.filter_size_snow(0)
        array_after = snowmap.get_array()
        self.assertTrue(np.abs(array_before-array_after).sum() == 0)

        for min_cluster_size in np.linspace(10, 100, 4).astype(int):
            sm = snowmap.copy()
            sm.filter_size_snow(min_cluster_size)
            # Testing whether there's any cluster smaller than the testsize
            snow_clusters, num_clusters = measurements.label(sm.get_array()==1,
                    structure=sm._structure)
            cluster_indices, counts = np.unique(snow_clusters, return_counts=True)
            msk_nonzero = cluster_indices != 0 # cluster 0 contains no snow, no need to include in count
            self.assertTrue(counts[msk_nonzero].min() >= min_cluster_size)
    def test_read_snowline_2(self):
        dirpath=os.path.join(
                os.path.abspath(os.path.dirname(__file__)), 
                'data') +'/'
        filenames = [
"L2SNOW_reproj_idepix_subset_S3A_OL_1_EFR____20191214T093535_20191214T093835_20191215T142923_0179_052_307_2160_LN1_O_NT_002.SEN3.nc"
    ]
        for filename in filenames:
            SnowMap.from_netcdf(dirpath+filename)



class TestUpdate(unittest.TestCase):
    def test_update1(self):
        grid = Grid()
        nullmap = grid.zeros()
        changemap = np.random.choice(np.arange(-1,2), 
                    size=nullmap.shape).astype('int8')
        usm = UpdatedSnowMap(array=nullmap, is_internal=True)
        with self.assertRaises(TypeError):
            usm.update(changemap)
        with self.assertRaises(ValueError):
            usm.update(SnowMap(changemap, is_internal=False))
        usm.update(SnowMap(changemap, is_internal=True))
        # Since the map was originally only 0, the update is equal to the new map
        self.assertTrue(np.abs(usm.get_array()-changemap).sum() == 0)

    def test_update2(self):
        grid = Grid()
        nullmap = grid.zeros()
        filename = os.path.join(
                os.path.abspath(os.path.dirname(__file__)), 
                'data',
'L2SNOW_reproj_idepix_subset_S3A_OL_1_EFR____20191214T093535_20191214T093835_20191215T142923_0179_052_307_2160_LN1_O_NT_002.SEN3.nc'
            )
        snowmap_ntf = SnowMap.from_netcdf(filename, transform=True)
        usm = UpdatedSnowMap(array=nullmap, is_internal=True)
        usm.update(snowmap_ntf)
        # Since the map was originally only 0, the update is equal to the new map
        self.assertTrue(np.all(snowmap_ntf.get_array() == usm.get_array) == 0)

class TestSaveLoad(unittest.TestCase):
    def test_save_load_1(self):
        grid = Grid()
        nullmap = grid.zeros()
        randommap = np.random.choice(np.arange(-1,2),
                    size=nullmap.shape).astype('int8')
        filename = 'smap.tar.gz'
        for is_internal in (True, False):
            attributes =  dict(is_internal=is_internal)
            # ~ with tempfile.TemporaryFile() as filename:
            smap_old = SnowMap(randommap, **attributes)
            smap_old.save(filename)
            smap_new = SnowMap.load(filename)

            self.assertTrue(
                np.all(smap_new.get_array() == smap_old.get_array()))
            self.assertTrue(
                np.all(smap_new.get_array() == randommap))
            self.assertTrue(smap_new._get_attributes(
                    ) == smap_new._get_attributes() == attributes)
            os.remove(filename)
if __name__ == '__main__':
    unittest.main()
