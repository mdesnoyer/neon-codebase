#!/usr/bin/env python
'''Quick script that describes the model in a file.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
USAGE='%prog <model_file>'

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import model
from optparse import OptionParser
import re

def PrettyPrintModel(mod):
    brackets = []
    outString = ''
    indent = 0
    for c in str(mod):
        if c == '{' or c == '[':
            indent += 2
            outString += '%s\n%s' % (c,' '*indent)
            brackets.append(c)
            continue
        elif c == '}' or c == ']':
            indent -= 2
            outString += '\n%s' % (' '*indent)
            brackets.pop()
        elif c == '(':
            brackets.append(c)
        elif c == ')':
            brackets.pop()
        elif c == ',':
            if brackets[-1] <> '(':
                outString += '\n%s' % (' '*indent)
                continue
        outString += c

    print outString        

if __name__ == '__main__':
    parser = OptionParser(usage=USAGE)
    
    options, args = parser.parse_args()

    mod = model.load_model(args[0])

    PrettyPrintModel(mod)
