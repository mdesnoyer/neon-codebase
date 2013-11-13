#!/usr/bin/env python

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
        sys.path.insert(0,base_path)

import unittest
from supportServices.neondata import *

class TestSingleton(unittest.TestCase):
    def setUp(self):
        bp = BrightcovePlatform('2','3',4)
        self.bp_conn = DBConnection(bp)

        bp2 = BrightcovePlatform('12','13',4)
        self.bp_conn2 = DBConnection(bp2)


        vm = VideoMetadata('test1',None,None,None,None,None,None,None)
        self.vm_conn = DBConnection(vm)

        vm2 = VideoMetadata('test2',None,None,None,None,None,None,None)
        self.vm_conn2 = DBConnection(vm2)

    def test_instance(self):
        self.assertEqual(self.bp_conn,self.bp_conn2)
        self.assertEqual(self.vm_conn,self.vm_conn2)

        self.assertNotEqual(self.bp_conn,self.vm_conn)

if __name__ == '__main__':
    unittest.main()

