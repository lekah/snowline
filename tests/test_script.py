import unittest
import os, numpy as np
from snowline.analysis.snowmap import SnowMap, UpdatedSnowMap
from snowline.analysis.grid import Grid
from snowline.bin.update_snowmap import update_snowmap

class TestUpdateScript(unittest.TestCase):
    def test_script_1(self):
        netcdf_file_path = os.path.join(
                os.path.abspath(os.path.dirname(__file__)), 
'data','L2SNOW_reproj_idepix_subset_S3A_OL_1_EFR____20191214T093535_20191214T093835_20191215T142923_0179_052_307_2160_LN1_O_NT_002.SEN3.nc'
            )
        new_update_map_path = 'newmap.tar.gz'
        with self.assertRaises(OSError):
            update_snowmap(None, netcdf_file_path,
                new_update_map_path=None, allow_start_zeros=False, dry_run=True)
        with self.assertRaises(OSError):
            update_snowmap("Nonextistn", netcdf_file_path,
                new_update_map_path=None, allow_start_zeros=False, dry_run=True)
        update_snowmap(None, netcdf_file_path=netcdf_file_path,
                new_update_map_path=new_update_map_path,
                allow_start_zeros=True, dry_run=True,
                size_filter_snow=100, size_filter_nonsnow=0)
        
        self.assertTrue(new_update_map_path, os.listdir('./'))
        os.remove(new_update_map_path)
if __name__ == '__main__':
    unittest.main()
    
