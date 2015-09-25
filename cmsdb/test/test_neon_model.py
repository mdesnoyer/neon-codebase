import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] != __base_path__:
        sys.path.insert(0, __base_path__)
import test_utils.neontest
import unittest
#import testing.postgresql
import test_utils.postgresql
from cmsdb import neon_model

class TestNeonUserAccount(test_utils.neontest.AsyncTestCase):
    def setUp(self): 
        self.postgresql = test_utils.postgresql.Postgresql()
    def tearDown(self):
        self.postgresql.stop()

    def test_neon_api_key(self):
        so = neon_model.StoredObject('new_key')
        so.save() 
        self.assertEquals(1,1) 

if __name__ == "__main__" :
    utils.neon.InitNeon()
    unittest.main()
