#!/usr/bin/env python
'''A script that replays a thrift file and sends it to flume.

**** WARNING **** This script loads all the events in each file to memory.
if they are very big then you might run out of memory.
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from clickTracker.flume import ThriftSourceProtocol
from clickTracker.flume.ttypes import *
from clickTracker import TTornado
import os
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol
import tornado.ioloop
import tornado.gen
import utils.neon

import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define("flume_port", default=6367, type=int,
       help='Port to talk to the flume agent running locally')

def connect_to_flume():
    transport = TTornado.TTornadoStreamTransport('localhost',
                                                 options.flume_port)
    pfactory = TCompactProtocol.TCompactProtocolFactory()
    flume = ThriftSourceProtocol.Client(transport, pfactory)
    return flume, transport

@tornado.gen.coroutine
def process_file(cur_file):
    flume, sock = connect_to_flume()

    try:
        _log.info('Processing file %s' % cur_file)

        # TODO(mdesnoyer): Stream the events to flume so that this
        # script doesn't run out of memory.
        events = []
        with open(cur_file, 'rb') as in_file:
            in_stream = TTransport.TFileObjectTransport(in_file)
            protocol_reader = TCompactProtocol.TCompactProtocol(in_stream)

            try:
                while True:
                    event = ThriftFlumeEvent()
                    event.read(protocol_reader)
                    if len(event.headers) == 0:
                        break
                    events.append(event)
            except EOFError:
                pass

        _log.info("Finished reading %s, sending to flume" % cur_file)
        status = yield tornado.gen.Task(flume.appendBatch, events)
        if status != Status.OK:
            raise Thrift.TException('Flume returned error: %s' % status)

        _log.info("Sucessfully replayed %s. Deleting the file" %
                  cur_file)
        os.remove(cur_file)
    finally:
        sock.close()

@tornado.gen.coroutine
def main(input_files):
    exception = None
    for cur_file in input_files:
        try:
            yield process_file(cur_file)
        except Exception as e:
            _log.exception("Error processing file %s: %s" % (cur_file, e))
            exception = e

    # Make sure that the process exits with an error code
    if exception is not None:
        raise exception
    

if __name__ == '__main__':
    input_files = utils.neon.InitNeon()
    tornado.ioloop.IOLoop.instance().run_sync(lambda: main(input_files))
