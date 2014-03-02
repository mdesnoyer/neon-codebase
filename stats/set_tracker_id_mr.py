#!/usr/bin/env python
'''Map reduce job that sets the tracker id in the logs for a given domain.


Takes the logs from one bucket, sets the tracker id for all entries
from a specific domain, and then writes the logs to an output bucket.

You will probably run this from the command line with something like:

./set_tracker_id_mr.py -c /path/to/mr/conf --runner emr 
--tracker_id 1483115066 --domain post-gazette.com -o s3://output-bucket/folder/ 
s3://input-bucket

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)
    
import json
import logging
from mrjob.job import MRJob
import mrjob.protocol
import mrjob.util
import re

_log = logging.getLogger(__name__)

class SetTrackerID(MRJob):
    INPUT_PROTOCOL = mrjob.protocol.RawProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.RawProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol 

    def configure_options(self):
        super(SetTrackerID, self).configure_options()
        self.add_passthrough_option(
            '--tracker_id', default=None,
            help='The tracker id to write in those entries')
        
        self.add_passthrough_option(
            '--domain',  default=None,
            help=('The domain of the log entries to match against. '
                  'This will be a regex search'))

    def mapper(self, line, _):
        '''Reads the json log and outputs the logs with new tracker id.
        
        '''
        try:
            data = json.loads(line)
            if re.compile(self.options.domain).search(data['page']):
                data['tai'] = self.options.tracker_id
                self.increment_counter('SetTrackerIDInfo',
                                       'EntriesUpdated',
                                       1)

            yield json.dumps(data), ''
            
        except ValueError as e:
            _log.error('JSON could not be parsed: %s' % line)
            self.increment_counter('SetTrackerIDError',
                                   'JSONParseErrors', 1)
        except KeyError as e:
            _log.error('Input data was missing a necessary field (%s): %s' % 
                       (e, line))
            self.increment_counter('SetTrackerIDError',
                                   'JSONFieldMissing', 1)

def main():
    # Setup a logger for dumping errors to stderr
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter('%(asctime)s %(levelname)s:%(name)s %(message)s')
    _log.addHandler(handler)
    _log.setLevel(logging.WARNING)
    
    SetTrackerID.run()

if __name__ == '__main__':
    main()
