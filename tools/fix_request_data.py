#!/usr/bin/env python
'''Script that fixes request data in the database

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2013 Neon Labs
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> __base_path__:
    sys.path.insert(0, __base_path__)

from cmsdb import neondata
import utils.neon

from utils.options import define, options

def main():
    db_connection = neondata.DBConnection('NeonApiRequest')

    # Get the request keys
    keys = db_connection.blocking_conn.keys('request_*')
    for key in keys:
        request_json = db_connection.blocking_conn.get(key)
        request = neondata.NeonApiRequest.create(request_json)

        # Fix when the finished requests got labeled as internal_video_id
        if request.state == 'internal_video_id':
            request.state = neondata.RequestState.FINISHED
            request.save()

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
