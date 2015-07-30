#!/usr/bin/env python
'''
Script that submits a lot of Discovery jobs for backprocessing

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2015
'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.brightcove_api
from cmsdb import neondata
import logging
import utils.http
import utils.neon

from utils.options import define, options
define('cmsapi_host', default='services.neon-lab.com'
       help='Host where the cmsapi is')
define('max_submit_rate', default=60.0,
       help='Maximum number of jobs to submit per hour')

_log = logging.getLogger(__name__)

def main():
    plat = neondata.BrightcovePlatform.get('gvs3vytvg20ozp78rolqmdfa', '71')

if __name__ == '__main__':
    utils.neon.InitNeon()
