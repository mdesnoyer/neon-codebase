'''
Service that handles a callback and pushes the serving url to the brightcove 
account.

Authors: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2016 Neon Labs
'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from api import brightcove_api
from cmsdb import neondata
import json
import logging
import signal
import tornado.gen
import tornado.web
import utils.neon
from utils.options import define
import utils.ps
from utils import statemon

define('port', default=8888, help='Port to bind to')

_log = logging.getLogger(__name__)

class ServingURLHandler(tornado.web.RequestHandler):
    '''Handler for a callback that will push a serving url.''' 

    @tornado.gen.coroutine
    def put(self, integration_id):
        bc_integration = yield neondata.BrightcoveIntegration.get(
            integration_id)
        if bc_integration is None:
            raise tornado.web.HTTPError(404, 'Invalid integration id')

        if not bc_integration.uses_bc_thumbnail_api:
            # Nothing to do because they don't use the BC thumbnails
            self.set_status(200)
            return

        data = json.loads(self.request.body)

        if (bc_integration.application_client_id is None or
            bc_integration.application_client_secret is None):
            yield self.handle_callback_with_media_api(data, integration)
        else:
            yield self.handle_callback_with_cms_api(data, integration)

        self.set_status(200)

    @tornado.gen.coroutine
    def handle_callback_with_cms_api(self, data, integration):
        api = brightcove_api.CMSAPI(integration)
        if data['processing_state'] == 'serving':
            # First 
        elif data['processing_state'] == 'processed':
            # Replace the original image
            pass

    @tornado.gen.coroutine
    def push_one_serving_url_cms(self, data, api, asset_name):
        '''Push the serving url for one asset type on the cms api.

        data - Callback data json
        asset_name - 'thumbnail' or 'poster'
        '''
        get_func = api.get_attr('get_%s_list' % asset_name)
        add_func = api.get_attr('add_%s' % asset_name)

        cur_asset = yield get_func(data['video_id'])

        # TODO(mdesnoyer): Figure out if I need to us Add or Update

    @tornado.gen.coroutine
    def handle_callback_with_media_api(self, data, integration)
        api = brightcove_api.BrightcoveApi(integration.account_id,
                                           integration.publisher_id,
                                           integration.read_token,
                                           integration.write_token)
        if data['processing_state'] == 'serving':
            # First 
        elif data['processing_state'] == 'processed':
            # Replace the original image
            pass
    


   
        

class HealthCheckHandler(tornado.web.RequestHandler):
    '''Handler for health check ''' 

    @tornado.gen.coroutine
    def get(self, *args, **kwargs):
        self.set_status(200)
        self.write("<html> Server OK </html>")
        self.finish()

application = tornado.web.Application([
    (r'/healthcheck/?$', HealthCheckHandler),
    (r'/update_serving_url/([^/]*)/?$', ServingURLHandler)
    ], gzip=True)

def main():
    server = tornado.httpserver.HTTPServer(application)
    utils.ps.register_tornado_shutdown(server) 
    server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
