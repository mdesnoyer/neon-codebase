#!/usr/bin/env python
'''A client to talk to the api v2.

Deals with authentication etc automatically.

Can be called as a script, in which case, it expects:
cmsdb_query.py [options] <URL> [<json body>]

URL can be relative to the host.

Copyright: 2016 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

USAGE = '%prog [options] <URL> [<json body>]'

import os.path
import sys
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))
import cmsapiv2.client
import logging
import simplejson as json
import tornado.gen
import tornado.httpclient
import utils.neon
import utils.sync
from utils.options import define, options

define("user", default=None, type=str, 
       help="Username to connect with")
define("pwd", default=None, type=str, 
       help="Password")
define("method", default=None, type=str,
       help=("When used as a command line tool, what http method should be "
             "called"))

_log = logging.getLogger(__name__)

@utils.sync.optional_sync
@tornado.gen.coroutine
def main():
    args = utils.neon.InitNeon(USAGE)

    if len(args) == 1:
        url = args[0]
        body = None
        method = 'GET'
        headers = {}
    elif len(args) == 2:
        url = args[0]
        body = json.dumps(json.loads(args[1]))
        method = options.method or 'POST'
        headers = {'Content-Type': 'application/json'}
    else:
        _log.error('Invalid input. Must be %s' % USAGE)
        return

    client = cmsapiv2.client.Client(options.user, options.pwd)

    request = tornado.httpclient.HTTPRequest(
        url,
        method=method,
        body=body,
        headers=headers)
    response = yield client.send_request(request, ntries=1)

    if response.error:
        _log.error('Error sending request (%s): %s' %
                   (response.code, response.body))
        return

    _log.info('Response (%s): %s' % (response.code, response.body))
    

if __name__ == '__main__':
    main()
    
