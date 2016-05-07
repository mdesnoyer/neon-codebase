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
import urllib
import urlparse
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

        # Get the current images on the video
        images = yield api.get_video_images(data['video_id'])
        if data['processing_state'] == 'serving':
            yield self._push_one_serving_url_cms(data, api, integration,
                                                 'poster', images)
            yield self._push_one_serving_url_cms(data, api, integration,
                                                 'thumbnail', images)
        elif data['processing_state'] == 'processed' and len(images) > 0:
            # Replace the original image if our serving url was there before
            cur_img = images.values()[0]
            if (cur_img['remote'] and 'neon-images.com' in cur_img['src']):
                # It's not our image. JUMP
                return
            turls = yield self._choose_replacement_thumb(data, integration)
            yield self._push_static_url_cms(api, integration, turls,
                                            'poster', images)
            yield self._push_static_url_cms(api, integration, turls,
                                            'thumbnail', images)

    def _find_image_size(self, image_response):
        '''Returns (width, height) of the image in a Brightcove Response.'''
        width = None
        height = None

        if len(image_response) == 0:
            return (None, None)

        if image_response['remote']:
            # Try to get the image size from the URL
            query_params = urlparse.parse_qs(
                urlparse.urlparse(image_response['src']).query)
            try:
                height = int(query_params.get('height') or 
                             query_params.get('h'))
            except ValueError:
                pass
            try:
                width = int(query_params.get('width') or 
                            query_params.get('w'))
            except ValueError:
                pass
        else:
            width = max(x['width'] for x in image_response['sources'])
            height = max(x['height'] for x in image_response['sources'])

        return (width, height)

    @tornado.gen.coroutine
    def _choose_replacement_thumb(data, integration):
        '''Get the ThumbnailServingURL object for the thumbnail to put back 
        in Brightcove'''

        # First choose the thumbnail by getting a default, followed by
        # the best Neon one if a default doesn't exist.
        video = yield neondata.VideoMetadata.get(
            neondata.InternalVideoID.generate(integration.account_id,
                                              data['video_id']),
                                              async=True)
        if video is None:
            raise tornado.web.HTTPError(404, 'Unknown video id')
        thumbs = yield neondata.ThumbnailMetadata.get_many(
            video.thumbnail_ids, async=True)
        valid_thumbs = sorted([x for x in thumbs if 
                               x.type==neondata.ThumbnailType.DEFAULT],
                               key=lambda x: x.rank)
        if len(valid_thumbs) == 0:
            valid_thumbs = sorted([x for x in thumbs if 
                                   x.type==neondata.ThumbnailType.NEON],
                                   key=lambda x: x.rank)
            if len(valid_thumbs) == 0:
                valid_thumbs = thumbs
        tmeta = valid_thumbs[0]

        turls = yield neondata.ThumbnailServingURL.get(tmeta.key, async=True)
        raise tornado.gen.Return(turls)

    @tornado.gen.coroutine
    def _push_static_url_cms(api, integration, turls, asset_name, images):
        '''Push our desired url into Brightcove.'''
        cur_image = cur_images.get(asset_name, {})
        width, height = self._find_image_size(cur_image)

        if len(cur_image) == 0:
            return
        
        try:
            new_url = turls.get_serving_url(width, height)
        except KeyError:
            _log.warn_n('Could not find serving url of size %s,%s for '
                        'thumb %s' % (width, height, turls.get_id()))
            # TODO(mdesnoyer): Pick a size

        update_func = api.get_attr('update_%s' % asset_name)
        yield update_func(cur_image['asset_id'], new_url)

    @tornado.gen.coroutine
    def _push_one_serving_url_cms(self, data, api, integration, asset_name,
                                 cur_images):
        '''Push the serving url for one asset type on the cms api.

        data - Callback data json
        asset_name - 'thumbnail' or 'poster'
        '''
        cur_image = cur_images.get(asset_name, {})
        width, height = self._find_image_size(cur_image)

        if len(cur_image) > 0:
            # First we need to see if we already have the image in our
            # system and ingest it if we do not               
            video = yield neondata.VideoMetadata.get(
                neondata.InternalVideoID.generate(integration.account_id,
                                                  data['video_id']),
                                                  async=True)
            if video is None:
                raise tornado.web.HTTPError(404, 'Unknown video id')
            thumbs = yield neondata.ThumbnailMetadata.get_many(
                video.thumbnail_ids, async=True)
            min_rank = min([x.rank for x in thumbs if 
                            x.type == neondata.ThumbnailType.DEFAULT])

            if min_rank is None:
                # ingest the image
                new_thumb = neondata.ThumbnailMetadata(
                    None,
                    ttype=neondata.ThumbnailType.DEFAULT,
                    rank=0,
                    external_id=cur_image['asset_id'])
                new_thumb = yield video.download_and_add_thumbnail(
                    new_thumb,
                    cur_image['src'],
                    save_objects=True,
                    async=True)

           if cur_image['remote']:
               # We can update the remote url
               update_func = api.get_attr('update_%s' % asset_name)
               yield update_func(cur_image['asset_id'],
                                 self.build_serving_url(
                                     data['serving_url'], height, width))
               return
            else:
               # Delete it from Brightcove
               delete_func = api.get_attr('delete_%s' % asset_name)
               yield delete_func(data['video_id'], cur_image['asset_id'])

        # Add a new thumbnail with a remote url
        add_func = api.get_attr('add_%s' % asset_name)
        yield add_func(self.build_serving_url(data['serving_url'], height,
                                              width))

    def build_serving_url(self, base_url, height=None, width=None):
        parsed_url = list(urlparse.urlparse(base_url))
        query_string = urlparse.parse_qs(parsed_url[4])
        if height is not None:
            query_string['height'] = height
        if width is not None:
            query_string['width'] = width
        parsed_url[4] = urllib.urlencode(query_string)
        return urlparse.urlunparse(parsed_url)

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
