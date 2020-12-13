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
        new_state_map = 'newmap.tar.gz'
        with self.assertRaises(OSError):
            update_snowmap(state_map=None,
                netcdf_files=[netcdf_file_path],
                new_state_map=None, allow_blank=False, dry_run=True)
        with self.assertRaises(OSError):
            update_snowmap(state_map="Nonextistn",
                netcdf_files=[netcdf_file_path],
                new_state_map=None, allow_blank=False, dry_run=True)
        update_snowmap(state_map=None,
                netcdf_files=[netcdf_file_path],
                new_state_map=new_state_map,
                allow_blank=True, dry_run=True, no_boundaries=True,
                quiet=True,  
            )
        
        self.assertTrue(new_state_map, os.listdir('./'))
        os.remove(new_state_map)
if __name__ == '__main__':
    unittest.main()
    
