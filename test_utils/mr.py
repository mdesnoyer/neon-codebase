'''Tools for testing map reduce jobs.'''

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

from mrjob.protocol import *
from StringIO import StringIO

def run_single_step(mr, input_str, step_type='mapper', step=0,
                    protocol=PickleProtocol):
    '''Runs a single MapReduce step and returns the results.

    Inputs:
    mr - Map reduce job
    input_str - stdin input string to process
    step - Step to run
    step_type - 'mapper' or 'reducer'
    protocol - Protocole that the input data was encoded as
    
    Outputs: ([(key, value)], counters)
    '''
    results = []
    counters = {}

    stdin = StringIO(input_str)
    mr.sandbox(stdin=stdin)
    if step_type == 'mapper':
        if step == 0:
            mr.INPUT_PROTOCOL = protocol
        else:
            mr.INTERNAL_PROTOCOL = protocol
        mr.run_mapper(step)
        return (mr.parse_output(mr.INTERNAL_PROTOCOL()),
                mr.parse_counters())
    elif step_type == 'reducer':
        mr.INTERNAL_PROTOCOL = protocol
        mr.run_reducer(step)
        if step == len(mr.steps()) - 1:
            return (mr.parse_output(mr.OUTPUT_PROTOCOL()),
                    mr.parse_counters())
        else:
            return (mr.parse_output(mr.INTERNAL_PROTOCOL()),
                    mr.parse_counters())
