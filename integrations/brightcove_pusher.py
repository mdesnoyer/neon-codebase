#!/usr/bin/env python
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

import api.brightcove_api
from cmsdb import neondata
from cvutils.imageutils import PILImageUtils
import json
import logging
import re
import signal
import tornado.gen
import tornado.httpserver
import tornado.web
import urllib
import urlparse
import utils.neon
from utils.options import define, options
import utils.ps
from utils import statemon

define('port', default=8888, help='Port to bind to')

statemon.define('callbacks_received', int)
statemon.define('serving_urls_pushed', int)
statemon.define('originals_returned', int)
statemon.define('invalid_json', int)
statemon.define('no_valid_tokens', int)
statemon.define('bc_server_error', int)
statemon.define('unexpected_error', int)
statemon.define('image_ingestion_error', int)

_log = logging.getLogger(__name__)

class ServingURLHandler(tornado.web.RequestHandler):
    '''Handler for a callback that will push a serving url.''' 

    @tornado.gen.coroutine
    def put(self, integration_id):
        statemon.state.increment('callbacks_received')
        _log.info('Received callback for integration id: %s' % integration_id)
        bc_integration = yield neondata.BrightcoveIntegration.get(
            integration_id, async=True)
        if bc_integration is None:
            raise tornado.web.HTTPError(404, reason='Invalid integration id')

        if not bc_integration.uses_bc_thumbnail_api:
            # Nothing to do because they don't use the BC thumbnails
            self.write({'message': ('No change because account does not use '
                                    'the Brightcove thumbnails')})
            self.set_status(200)
            return

        try:
            data = json.loads(self.request.body)
        except Exception:
            statemon.state.increment('invalid_json')
            raise tornado.web.HTTPError(
                400, reason = 'Invalid JSON received: %s' % self.request.body)

        try:
            _log.info('Processing video %s (%s) integration id: %s' % 
                      (data['video_id'], data['processing_state'],
                       integration_id))
            if (bc_integration.application_client_id is not None and
                bc_integration.application_client_secret is not None):
                try:
                    yield self.handle_callback_with_cms_api(data,
                                                            bc_integration)
                    self.write({'message': ('Brightcove thumbnail successfuly '
                                            'updated')})
                    self.set_status(200)
                    return
                except api.brightcove_api.BrightcoveApiNotAuthorizedError as e:
                    # Try with the media API
                    pass

            if (bc_integration.read_token is not None and
                bc_integration.write_token is not None):
                yield self.handle_callback_with_media_api(data, bc_integration)
                self.write({'message': ('Brightcove thumbnail successfuly '
                                        'updated')})
                self.set_status(200)
                return

            raise api.brightcove_api.BrightcoveApiNotAuthorizedError(
                'No tokens in the integration object')

        except api.brightcove_api.BrightcoveApiServerError as e:
            msg = 'Error with the Brightcove API: %s' % e
            _log.error(msg)
            statemon.state.increment('bc_server_error')
            raise tornado.web.HTTPError(500, reason=msg)
        except api.brightcove_api.BrightcoveApiNotAuthorizedError as e:
            msg = ('No valid Brightcove tokens for integration %s' %
                   integration_id)
            _log.warn_n(msg, 20)
            statemon.state.increment('no_valid_tokens')
            raise tornado.web.HTTPError(500, reason=msg)
        except tornado.web.HTTPError as e:
            raise
        except Exception as e:
            msg = 'Unexpected Error with request %s: %s' % (data, e)
            _log.exception(msg)
            statemon.state.increment('unexpected_error')
            raise tornado.web.HTTPError(500)

    @tornado.gen.coroutine
    def post(self, integration_id):
        response = yield self.put(integration_id)
        raise tornado.gen.Return(response)
        

    @tornado.gen.coroutine
    def handle_callback_with_cms_api(self, data, integration):
        api_conn = api.brightcove_api.CMSAPI(
            integration.publisher_id,
            integration.application_client_id,
            integration.application_client_secret)

        # Get the current images on the video
        images = yield api_conn.get_video_images(data['video_id'])
        if data['processing_state'] == 'serving':
            yield self._ingest_existing_image(data, integration, images)
            yield self._push_one_serving_url_cms(data, api_conn, integration,
                                                 'poster', images)
            yield self._push_one_serving_url_cms(data, api_conn, integration,
                                                 'thumbnail', images)
            statemon.state.increment('serving_urls_pushed')
        elif data['processing_state'] == 'processed' and len(images) > 0:
            # Replace the original image if our serving url was there before
            cur_img = images.values()[0]
            if (not cur_img['remote'] or 'neonvid_' not in cur_img['src']):
                # It's not our image. JUMP!
                return
            tmeta, turls = yield self._choose_replacement_thumb(data,
                                                                integration)
            yield self._push_static_url_cms(data, api_conn, integration, turls,
                                            'poster', images)
            yield self._push_static_url_cms(data, api_conn, integration, turls,
                                            'thumbnail', images)
            statemon.state.increment('originals_returned')

    @tornado.gen.coroutine
    def _find_image_size(self, image_response):
        '''Returns (width, height) of the image in a Brightcove Response.'''
        width = None
        height = None

        if len(image_response) == 0:
            raise tornado.gen.Return((None, None))

        if image_response['remote']:
            # Try to get the image size from the URL
            query_params = dict(urlparse.parse_qsl(
                urlparse.urlparse(image_response['src']).query))
            try:
                height = int(query_params.get('height') or 
                             query_params.get('h'))
            except TypeError:
                pass
            try:
                width = int(query_params.get('width') or 
                            query_params.get('w'))
            except TypeError:
                pass
        else:
            width = max([x.get('width') for x in image_response['sources']] or
                        [None])
            height = max([x.get('height') for x in image_response['sources']]
                         or [None])

        if (width is None or height is None):
            width, height = yield self._get_size_from_existing_image(
                image_response)

        raise tornado.gen.Return((width, height))

    @tornado.gen.coroutine
    def _choose_replacement_thumb(self, data, integration):
        '''Get the thumbnail to put back into Brightcove.

        Returns (ThumbnailMetadata, ThumbnailServingURLs)
        '''

        # First choose the thumbnail by getting a default, followed by
        # the best Neon one if a default doesn't exist.
        video = yield neondata.VideoMetadata.get(
            neondata.InternalVideoID.generate(integration.account_id,
                                              data['video_id']),
                                              async=True)
        if video is None:
            raise tornado.web.HTTPError(404, reason='Unknown video id')
        thumbs = yield neondata.ThumbnailMetadata.get_many(
            video.thumbnail_ids, async=True)
        valid_thumbs = sorted([x for x in thumbs if 
                               x and x.type==neondata.ThumbnailType.DEFAULT],
                               key=lambda x: x.rank)
        if len(valid_thumbs) == 0:
            valid_thumbs = sorted([x for x in thumbs if 
                                   x.type==neondata.ThumbnailType.NEON],
                                   key=lambda x: x.rank)
            if len(valid_thumbs) == 0:
                valid_thumbs = thumbs

                if len(valid_thumbs) == 0:
                    _log.warn_n('No thumbnails found for video %s' % video.key)
        tmeta = valid_thumbs[0]

        turls = yield neondata.ThumbnailServingURLs.get(tmeta.key, async=True)
        if turls is None:
            raise tornado.web.HTTPError(
                500, reason='No thumbnail serving URLs known')
        raise tornado.gen.Return((tmeta, turls))

    @tornado.gen.coroutine
    def _push_static_url_cms(self, data, api_conn, integration, turls,
                             asset_name, images):
        '''Push our desired url into Brightcove.'''
        cur_image = images.get(asset_name, {})
        width, height = yield self._find_image_size(cur_image)
        if width is None or height is None:
            width, height = api.brightcove_api.DEFAULT_IMAGE_SIZES[asset_name]

        if len(cur_image) == 0:
            # No image of this type to update
            return
        
        try:
            new_url = turls.get_serving_url(width, height)
        except KeyError:
            _log.warn_n('Could not find serving url of size %s,%s for '
                        'thumb %s' % (width, height, turls.get_id()))
            # Pick the best size we can get
            desired_size = (width, height)
            valid_sizes = turls.sizes.union(turls.size_map.iterkeys())
            if len(valid_sizes) == 0:
                _log.warn('No valid sizes for thumb %s' % turls.get_id())
                raise KeyError('No valid sizes to serve')
            mindiff = min([abs(x[0] - desired_size[0]) +
                           abs(x[1] - desired_size[1]) 
                           for x in valid_sizes])
            closest_size = [x for x in valid_sizes if
                        (abs(x[0] - desired_size[0]) +
                         abs(x[1] - desired_size[1])) == mindiff][0]
            new_url = turls.get_serving_url(*closest_size)

        update_func = getattr(api_conn, 'update_%s' % asset_name)
        yield update_func(data['video_id'], cur_image['asset_id'], new_url)

    @tornado.gen.coroutine
    def _ingest_existing_image(self, data, integration, bc_images):
        '''See if we already have the image in our system and ingest it 
        if we do not
        '''
        # Try for the poster first, then fallback to the thumbnail
        cur_image = bc_images.get('poster') or bc_images.get('thumbnail')
        if cur_image is None:
            return
        
        video = yield neondata.VideoMetadata.get(
            neondata.InternalVideoID.generate(integration.account_id,
                                              data['video_id']),
                                              async=True)
        if video is None:
            raise tornado.web.HTTPError(404, reason='Unknown video id')
        thumbs = yield neondata.ThumbnailMetadata.get_many(
            video.thumbnail_ids, async=True)

        if not any([x.type == neondata.ThumbnailType.DEFAULT 
                    for x in thumbs]):
            # ingest the image
            new_thumb = neondata.ThumbnailMetadata(
                None,
                ttype=neondata.ThumbnailType.DEFAULT,
                rank=0,
                external_id=cur_image['asset_id'])
            try:
                new_thumb = yield video.download_and_add_thumbnail(
                    new_thumb,
                    cur_image['src'],
                    save_objects=True,
                    async=True)
            except Exception as e:
                _log.warn('Error while ingesting image: %s' % e)
                statemon.state.increment('image_ingestion_error')
                               

    @tornado.gen.coroutine
    def _push_one_serving_url_cms(self, data, api_conn, integration,
                                  asset_name, cur_images):
        '''Push the serving url for one asset type on the cms api.

        data - Callback data json
        asset_name - 'thumbnail' or 'poster'
        '''
        cur_image = cur_images.get(asset_name, {})
        width, height = yield self._find_image_size(cur_image)
        if width is None or height is None:
            width, height = api.brightcove_api.DEFAULT_IMAGE_SIZES[asset_name]

        if len(cur_image) > 0:
            if cur_image['remote']:
               # We can update the remote url
               update_func = getattr(api_conn, 'update_%s' % asset_name)
               yield update_func(data['video_id'],
                                 cur_image['asset_id'],
                                 self.build_serving_url(
                                     data['serving_url'], height, width))
               return
            else:
               # Delete it from Brightcove
               delete_func = getattr(api_conn, 'delete_%s' % asset_name)
               yield delete_func(data['video_id'], cur_image['asset_id'])

        # Add a new thumbnail with a remote url
        add_func = getattr(api_conn, 'add_%s' % asset_name)
        yield add_func(data['video_id'],
                       self.build_serving_url(data['serving_url'], height,
                                              width))

    @tornado.gen.coroutine
    def _get_size_from_existing_image(self, image_response):
        '''Downloads the image in the account in order to get the image size.
        '''
        neonServingRe = re.compile('neon-images.com/v1/client')
        if ('src' not in image_response or 
            image_response['src'] is None or
            neonServingRe.search(image_response['src']) is not None):
            raise tornado.gen.Return((None, None))

        try:
            img = yield PILImageUtils.download_image(image_response['src'],
                                                     async=True)
        except Exception as e:
            _log.warn('Error downloading image %s: %s' %
                       (image_response['src'], e))
            raise tornado.gen.Return((None, None))

        raise tornado.gen.Return(img.size)
        

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
    def handle_callback_with_media_api(self, data, integration):
        api_conn = api.brightcove_api.BrightcoveApi(integration.account_id,
                                                    integration.publisher_id,
                                                    integration.read_token,
                                                    integration.write_token)
        # TODO: Handle the image sizes better. We're not going to
        # bother for now because the media API is deprecated and there
        # shouldn't be anybody using this code.
        
        if data['processing_state'] == 'serving':
            # Push in the servering url
            yield api_conn.update_thumbnail_and_videostill(
                data['video_id'],
                None,
                remote_url=data['serving_url'])
            statemon.state.increment('serving_urls_pushed')
        elif data['processing_state'] == 'processed':
            # Put the original image page
            tmeta, turls = yield self._choose_replacement_thumb(data,
                                                                integration)
            cur_error = None
            for url in tmeta.urls:
                try:
                    image = yield PILImageUtils.download_image(url, async=True)
                    cur_error = None
                    break
                except Exception as e:
                    cur_error = e
            if cur_error is not None:
                msg = 'Error while downloading thumbnail %s: %s' % (tmeta.key,
                                                                    cur_error)
                _log.warn(msg)
                raise tornado.web.HTTPError(500, reason=msg)

            yield api_conn.update_thumbnail_and_videostill(
                data['video_id'],
                tmeta.key,
                image=image)
            statemon.state.increment('originals_returned')
        

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
