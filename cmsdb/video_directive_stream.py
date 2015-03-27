'''
A stream of directives contained in a Kinesis stream

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2015 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import boto.kinesis
import json
import logging
import tornado.gen
import utils.botoutils
from utils.options import define, options
from utils import statemon
import utils.sync


_log = logging.getLogger(__name__)

define('region', default='us-east-1',
       help='Region where the kinesis stream is')
define('stream_name', default='default-thumb-serving-directives',
       help='Name of the Kinesis stream with the serving directives')

statemon.define('n_directives_sent', int)
statemon.define('n_directives_read', int)
statemon.define('kinesis_errors', int)
statemon.define('message_latency', float)

class DirectiveStream(object):
    def __init__(self):
        self._connection = None

    def connect():
        if self._connection is None:
            self._connection = boto.kinesis.connect_to_region(options.region)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def send():
