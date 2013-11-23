#!/usr/bin/env python
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import threading
import time
import unittest
import utils.neon
import utils.ps

class GarbageException(Exception):
    pass

class TestActivityWatcher(unittest.TestCase):
    def setUp(self):
        self.watcher = utils.ps.ActivityWatcher()

    def tearDown(self):
        pass

    def test_nested_activates(self):
        self.assertTrue(self.watcher.is_idle())
        self.assertFalse(self.watcher.is_active())

        with self.watcher.activate():
            self.assertTrue(self.watcher.is_active())
            self.assertFalse(self.watcher.is_idle())

            with self.watcher.activate():
                self.assertTrue(self.watcher.is_active())
                self.assertFalse(self.watcher.is_idle())

            self.assertTrue(self.watcher.is_active())
            self.assertFalse(self.watcher.is_idle())

        self.assertTrue(self.watcher.is_idle())
        self.assertFalse(self.watcher.is_active())

    def test_cleanup_after_exception(self):
        try:
            with self.watcher.activate():
                raise GarbageException()
                self.fail("Shouldn't get past exception")
        except GarbageException:
            pass

        self.assertTrue(self.watcher.is_idle())
        self.assertFalse(self.watcher.is_active())

    def test_exception_propagated(self):
        with self.assertRaises(GarbageException):
            with self.watcher.activate():
                raise GarbageException()
                self.fail("Shouldn't get past exception")

    def test_wait_for_idle(self):
        shared_val = [10]
        
        def increment_shared_val(shared_val):
            with self.watcher.activate():
                for i in range(10):
                    shared_val[0] += 1
                time.sleep(0.1)
                for i in range(12):
                    shared_val[0] += 1

        thread = threading.Thread(target=increment_shared_val,
                                  args=(shared_val,))
        thread.start()

        self.watcher.wait_for_active()
        self.watcher.wait_for_idle()

        self.assertEqual(shared_val[0], 32)
            

if __name__ == '__main__':
    utils.neon.InitNeonTest()
    unittest.main()
