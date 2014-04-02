'''
An extention of the builtin unittest module.

Allows some more complicated assert options.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''
from contextlib import contextmanager
import logging
import re
from StringIO import StringIO
import tornado.testing
import unittest

class TestCase(unittest.TestCase):
    '''Use this instead of the unittest one to get the extra functionality.'''

    @contextmanager
    def assertLogExists(self, level, regexp):
        '''Asserts that a log message was written at a given level.

        This can be used either in a with statement e.g:
        with self.assertLogExists(logging.INFO, 'Hi'):
          do_stuff()
          _log.info('Hi')

        or as a decorator. e.g.:
        @assertLogs(logging.INFO, 'hi')
        def test_did_log(self):
          _log.info('Hi')
          
        '''
        log_stream = StringIO()
        handler = logging.StreamHandler(log_stream)
        handler.setFormatter(logging.Formatter('%(message)s'))
        logger = logging.getLogger()
        logger.addHandler(handler)

        try:
            yield

        finally:
            logger.removeHandler(handler)
            handler.flush()
            reg = re.compile(regexp)
            found_log = False
            for line in log_stream.getvalue().split('\n'):
                if reg.search(line):
                    found_log = True
                    break
            if not found_log:
                self.fail('Msg: %s was not logged. The log was: %s' % 
                          (regexp, log_stream.getvalue()))

class AsyncTestCase(tornado.testing.AsyncTestCase, TestCase):
    '''A test case that has access to Neon functions and can do tornado async calls.'''
    pass

def main():
    unittest.main()
