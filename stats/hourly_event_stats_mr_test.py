#!/usr/bin/env python

import mrjob.protocol
import unittest
import StringIO

from hourly_event_stats_mr import *

class TestLogParsing(unittest.TestCase):
    def setUp(self):
        self.mr = HourlyEventStats(['-r', 'inline', '--no-conf', '-'])

    def _run_single_step(self, input_str, step_type='mapper', step=0,
                         protocol=mrjob.protocol.RawValueProtocol):
        '''Runs a single step and returns the results.

        Inputs:
        step - Step to run
        input_str - stdin input string to process

        Outputs: ([(key, value)], counters)
        '''
        results = []
        counters = {}

        stdin = StringIO.StringIO(input_str)
        self.mr.sandbox(stdin=stdin)
        if step_type == 'mapper':
            self.mr.INPUT_PROTOCOL = protocol
            self.mr.run_mapper(step)
            return (self.mr.parse_output(self.mr.INTERNAL_PROTOCOL()),
                    self.mr.parse_counters())
        elif step_type == 'reducer':
            self.mr.INTERNAL_PROTOCOL = protocol
            self.mr.run_reducer(step)
            return (self.mr.parse_output(self.mr.OUTPUT_PROTOCOL()),
                    self.mr.parse_counters())

    def test_valid_click(self):
        results, counters = self._run_single_step(
            ('{"sts":19800, "a":"click", "img":"http://monkey.com"}\n'
             '{"sts":19800, "a":"click", "img":"http://panda.com"}\n'))
        self.assertEqual(results, [(('click', 'http://monkey.com', 5), 1),
                                   (('click', 'http://panda.com', 5), 1)])

    def test_valid_load(self):
        self.assertEqual(
            [x for x in self.mr.mapper_get_events(
                '',('{"sts":19800, '
                    '"a":"load", '
                    '"imgs":["http://monkey.com",'
                    '"poprocks.jpg","pumpkin.wow"]}'))],
            [(('load', 'http://monkey.com', 5), 1),
             (('load', 'poprocks.jpg', 5), 1),
             (('load', 'pumpkin.wow', 5), 1)])

    def test_invalid_json(self):
        results, counters = self._run_single_step(
            ('{"sts":19800, "a":"click" "img":"http://monkey.com"}\n'
            '{"sts":1900, "a":"click", "img":"http://monkey.com"\n'
            '{"sts":1900, "a":"load", "imgs":["now.com"}\n'),
            step=0)
        self.assertEqual(results, [])
        self.assertEqual(
            counters['HourlyEventStatsErrors']['JSONParseErrors'], 3)

    def test_fields_missing(self):
        results, counters = self._run_single_step(
            ('{"a":"click", "img":"http://monkey.com"}\n'
             '{"sts":19800, "img":"http://monkey.com"}\n'
             '{"sts":19800, "a":"click"}\n'
             '{"sts":19800, "a":"click", "imgs":"http://monkey.com"}\n'
             '{"a":"load", "imgs":["a.com", "b.jpg", "c.png"]}\n'
             '{"sts":19800, "imgs":["a.com", "b.jpg", "c.png"]}\n'
             '{"sts":19800, "a":"load"}\n'
             '{"sts":19800, "a":"load", "img":["a.com", "b.jpg", "c.png"]}\n'
             '{"sts":19800, "a":"load", "imgs":"a.com"}\n'))
        self.assertEqual(results, [])
        self.assertEqual(
            counters['HourlyEventStatsErrors']['JSONFieldMissing'], 9)

if __name__ == '__main__':
    unittest.main()
