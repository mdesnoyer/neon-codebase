#!/usr/bin/env python
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

from mrjob.protocol import *
from StringIO import StringIO
import unittest
import utils.neon

from stats.set_tracker_id_mr import *

class TestSetTrackerID(unittest.TestCase):
    '''Tests database writing step.'''
    def setUp(self):
        self.mr = SetTrackerID(['-r', 'inline', '--no-conf', 
                                '--tracker_id', '24689', 
                                '--domain', 'post-gazette.com',
                                '-'])

    def tearDown(self):
        pass

    def test_no_change(self):
        # Setup the input data
        input_data = (
            '{"sts":19800, "a":"click", "page":"here.com", "tai":"tai_prod",'
            '"ttype":"flashonly", "img":"http://monkey.com"}\n'
            '{"sts":19795, "a":"load", "page":"here.com","ttype":"flashonly",'
            '"tai":"tai_prod", '
            '"imgs":["http://monkey.com","http://panda.com","pumpkin.wow"]}\n'
            '{"sts":19805, "a":"click", "page":"here.com", "tai":"tai_prod",'
             '"ttype":"flashonly", "img":"http://panda.com"}\n'
            '{"sts":19800, "a":"load", "page":"here.com", "tai":"tai_prod",'
            '"ttype":"flashonly","imgs":["http://monkey.com","pumpkin.jpg"]}\n'
            '{"sts":19810, "a":"click", "page":"here.com", "tai":"tai_prod",'
             '"ttype":"flashonly", "img":"http://panda.com"}\n'
            '{"sts":19810, "a":"click", "page":"here.com", "tai":"tai_prod",'
             '"ttype":"flashonly", "img":"pumpkin.jpg"}\n'
            '{"sts":19820, "a":"click", "page":"here.com", "tai":"tai_stage",'
            '"ttype":"flashonly", "img":"http://monkey.com"}')
        stdin = StringIO(input_data)
        self.mr.sandbox(stdin=stdin)

        with self.mr.make_runner() as runner:
            runner.run()
            self.assertEqual(runner.counters(), [{}])
            self.assertItemsEqual(
                [json.loads(x) for x in input_data.split('\n')],
                [json.loads(x.strip()) for x in runner.stream_output()])

    def test_add_tai(self):
        input_data = (
            '{"sts":19800, "a":"click", "page":"post-gazette.com", "tai":null,'
            '"ttype":"flashonly", "img":"http://monkey.com"}\n'
            '{"sts":19795, "a":"load", "page":"http://www.post-gazette.com",'
            '"ttype":"flashonly","tai":"246890",'
            '"imgs":["http://monkey.com","http://panda.com","pumpkin.wow"]}\n'
            '{"sts":19805, "a":"click", "page":"post-gazette.com/video", '
            '"tai":"tai_prod","ttype":"flashonly", "img":"http://panda.com"}')
        stdin = StringIO(input_data)
        self.mr.sandbox(stdin=stdin)

        with self.mr.make_runner() as runner:
            runner.run()
            self.assertEqual(
                runner.counters()[0]['SetTrackerIDInfo']['EntriesUpdated'], 3)
            for line in runner.stream_output():
                self.assertEqual(json.loads(line.strip())['tai'], '24689')

    def test_missing_page_entry(self):
        input_data = (
            '{"sts":19800, "a":"click", "tai":null,'
            '"ttype":"flashonly", "img":"http://monkey.com"}')
        stdin = StringIO(input_data)
        self.mr.sandbox(stdin=stdin)

        with self.mr.make_runner() as runner:
            runner.run()
            self.assertEqual(
                runner.counters()[0]['SetTrackerIDError']['JSONFieldMissing'],
                1)

    def test_invalid_json(self):
        input_data = ('{"sts":19800, "a":"click" "img":"http://monkey.com"}\n'
            '{"sts":1900, "a":"click", "img":"http://monkey.com"\n'
            '{"sts":1900, "a":"load", "imgs":["now.com"}\n')
        stdin = StringIO(input_data)
        self.mr.sandbox(stdin=stdin)

        with self.mr.make_runner() as runner:
            runner.run()
            self.assertEqual(
                runner.counters()[0]['SetTrackerIDError']['JSONParseErrors'],
                3)
            
if __name__ == '__main__':
    utils.neon.InitNeon()
    unittest.main()
