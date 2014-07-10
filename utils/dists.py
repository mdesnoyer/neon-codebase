'''
Utiltiles to calculate the distance between two things

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2014 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

def hamming_int(x, y):
    '''Calculates the hamming distance between two integers.

    The hamming distance is the number of bits that have changed.
    '''
    if x is None or y is None:
        return float('inf')
    return sum( b == '1' for b in bin(x ^ y)[2:] )
