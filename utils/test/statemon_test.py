#!/usr/bin/env python
'''
Testing the statemon module

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs

'''
import os.path
import sys
sys.path.insert(0,os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..')))

import multiprocessing
import utils.test.statemon_test_module as test_mod
import unittest
from utils import statemon

class TestStatemonObj(unittest.TestCase):
    def setUp(self):
        self.state = statemon.State()

    def test_simple_define(self):
        self.state.define('an_int', int)
        self.assertEqual(self.state.an_int, 0)
        self.state.an_int = 56
        self.assertEqual(self.state.an_int, 56)
        self.assertEqual(self.state['an_int'], 56)

        
        self.state.define('a_float', float)
        self.assertAlmostEqual(self.state.a_float, 0)
        self.state.a_float = 10.5
        self.assertAlmostEqual(self.state.a_float, 10.5)

        self.state.define('a_default_int', int, default=8)
        self.assertEqual(self.state.a_default_int, 8)
        self.state.a_default_int = 5
        self.assertEqual(self.state.a_default_int, 5)

    def test_define_and_increment(self):
        self.state.define_and_increment('an_int', 6)
        self.assertEqual(self.state.an_int, 6)

        self.state.define_and_increment('a_float', 6.6, float)
        self.assertAlmostEqual(self.state.a_float, 6.6, 5)

    def test_proper_increment(self):
        self.state.define('an_int', int)
        self.assertEqual(self.state.an_int, 0)
        self.state.increment('an_int', 2)
        self.assertEqual(self.state.an_int, 2)

        self.state.decrement('an_int')
        self.assertEqual(self.state.an_int, 1)

    def test_ref_increment(self):
        self.state.define('an_int', int)
        self.assertEqual(self.state.an_int, 0)
        ref = self.state.get_ref('an_int')
        self.state.increment(ref=ref, diff=2)
        self.assertEqual(self.state.an_int, 2)

        self.state.decrement(ref=ref)
        self.assertEqual(self.state.an_int, 1)

    def test_unsafe_increment(self):

        # Makes sure that the code path is tested, but won't actually
        # trigger the thread collision. That's way too hard to do
        # reliably :-)
        self.state.define('an_int', int)
        self.assertEqual(self.state.an_int, 0)
        ref = self.state.get_ref('an_int')
        self.state.increment(ref=ref, diff=2, safe=False)
        self.assertEqual(self.state.an_int, 2)

        self.state.decrement(ref=ref, safe=False)
        self.assertEqual(self.state.an_int, 1)

    def test_bad_increment_invokation(self):
        self.state.define('an_int', int)
        ref = self.state.get_ref('an_int')
        
        with self.assertRaises(TypeError):
            self.state.increment()

        with self.assertRaises(TypeError):
            self.state.decrement()

        with self.assertRaises(TypeError):
            self.state.increment(name='an_int', ref=ref)

        with self.assertRaises(TypeError):
            self.state.decrement(name='an_int', ref=ref)

    def test_unknown_variable(self):
        with self.assertRaises(AttributeError):
            self.state.an_int

        with self.assertRaises(AttributeError):
            self.state.a_float = 6

    def test_other_namespace(self):
        test_mod.define(self.state, 'an_int', int)
        test_mod.set(self.state, 'an_int', 56)
        self.assertEqual(test_mod.get(self.state, 'an_int'), 56)

        test_mod.define_and_increment(self.state, 'a_dynamic_int', 31)
        self.assertEqual(test_mod.get(self.state, 'a_dynamic_int'), 31)

    def test_invalid_types(self):
        with self.assertRaises(statemon.Error):
            self.state.define('a_string', str)

        with self.assertRaises(statemon.Error):
            self.state.define('a_bool', bool)

    def test_multiprocess_comms(self):
        self.state.define('an_int', int)
        done_increment = multiprocessing.Event()
        kill_proc = multiprocessing.Event()

        def do_increments():
            for i in range(5):
                self.state.increment('an_int')

            done_increment.set()
            kill_proc.wait()

        self.assertEqual(self.state.an_int, 0)
        proc = multiprocessing.Process(target=do_increments)
        proc.start()
        done_increment.wait()

        self.assertEqual(self.state.an_int, 5)
        kill_proc.set()
        proc.join()

    def test_multiprocess_comms_in_module(self):
        done_increment = multiprocessing.Event()
        kill_proc = multiprocessing.Event()
        
        proc = multiprocessing.Process(target=test_mod.run_increments,
                                       args=(done_increment, kill_proc))
        proc.start()
        try:
            done_increment.wait()

            self.assertEqual(statemon.state.get(
                'utils.test.statemon_test_module.mod_int'), 5)
        finally:
            kill_proc.set()
            proc.join()
        

if __name__ == '__main__':
    unittest.main()
