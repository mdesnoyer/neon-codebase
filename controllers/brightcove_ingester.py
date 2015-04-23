#!/usr/bin/env python
'''
Ingests changes from Brightcove into our system

For now, only identifies image changes. Will eventually ingest the new videos.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2015 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.brightcove_api
import atexit
from cmsdb import neondata
import datetime
import logging
import signal
import multiprocessing
import os.path
import re
import tornado.ioloop
import tornado.gen
import urlparse
import utils.neon
from utils.options import define, options
import utils.ps
from utils import statemon

define("poll_period", default=300, help="Period (s) to poll brightcove",
       type=int)

statemon.define('new_images_found', int)
statemon.define('cycles_complete', int)
statemon.define('bc_api_clienterror', int)
statemon.define('bc_api_servererror', int)
statemon.define('unexpected_exception', int)
statemon.define('video_not_found', int)
statemon.define('cant_get_image', int)
statemon.define('cycle_runtime', float)

_log = logging.getLogger(__name__)

def normalize_url(url):
    '''Returns a url without transport mechanism or query string.'''
    if url is None:
        return None
    parse = urlparse.urlparse(url)
    if re.compile('(brightcove)|(bcsecure)').search(parse.netloc):
        # Brightcove can move the image around, but its basename will
        # remain the same, so if it is a brightcove url, only look at
        # the basename.
        return 'brightcove.com/%s' % (os.path.basename(parse.path))
    return '%s%s' % (parse.netloc, parse.path)

def get_urls_from_bc_response(response):
    urls = []
    for image_type in ['thumbnail', 'videoStill']:
        urls.append(response.get(image_type + 'URL', None))
        if response.get(image_type, None) is not None:
            urls.append(response[image_type].get('remoteUrl', None))

    return [x for x in urls if x is not None]

def extract_image_info(response, field):
    '''Extracts a list of fields from the images in the response.'''
    vals = []
    for image_type in ['thumbnail', 'videoStill']:
        fields = response.get(image_type, None)
        if fields is not None:
            vals.append(fields.get(field, None))

    return [unicode(x) for x in vals if x is not None]

@tornado.gen.coroutine
def process_one_account(platform):
    '''Processes one Brightcove account.'''
    _log.debug('Processing Brightcove platform for account %s, integration %s'
               % (platform.neon_api_key, platform.integration_id))

    bc_api = platform.get_api()

    # Get information from Brightcove about all the videos we know about
    bc_video_ids = platform.get_videos()
    
    try:
        bc_video_info = yield bc_api.find_videos_by_ids(
            bc_video_ids,
            video_fields = ['id',
                            'videoStill',
                            'videoStillURL',
                            'thumbnail',
                            'thumbnailURL'],
            async=True)
    except api.brightcove_api.BrightcoveApiClientError as e:
        _log.error('Client error calling brightcove api for account %s, '
                   'integration %s. %s' % (platform.neon_api_key,
                                           platform.integration_id,
                                           e))
        statemon.state.increment('bc_api_clienterror')
        return
    except api.brightcove_api.BrightcoveApiServerError as e:
        _log.error('Server error calling brightcove api for account %s, '
                   'integration %s. %s' % (platform.neon_api_key,
                                           platform.integration_id,
                                           e))
        statemon.state.increment('bc_api_servererror')
        return

    for bc_video_id, data in bc_video_info.iteritems():
        # Get information from the response
        bc_thumb_ids = extract_image_info(data, 'id')
        bc_ref_ids = extract_image_info(data, 'referenceId')
        bc_urls = [normalize_url(x) for x in get_urls_from_bc_response(data)]

        # Function that will set the external id in the ThumbnailMetadata
        external_id = None
        if len(bc_thumb_ids) > 0:
            external_id = bc_thumb_ids[0]
        def _set_external_id(obj):
            obj.external_id = external_id
        
        # Process each video
        video_id = neondata.InternalVideoID.generate(platform.neon_api_key,
                                                     bc_video_id)
        vid_meta = yield tornado.gen.Task(neondata.VideoMetadata.get,
                                          video_id)
        if not vid_meta:
            _log.warn('Could not find video %s' % video_id)
            statemon.state.increment('video_not_found')
            continue

        thumbs = yield tornado.gen.Task(neondata.ThumbnailMetadata.get_many,
                                        vid_meta.thumbnail_ids)
        found_thumb = False
        min_rank = 1
        for thumb in thumbs:
            if (thumb is None or 
                thumb.type != neondata.ThumbnailType.BRIGHTCOVE):
                continue

            if thumb.rank < min_rank:
                min_rank = thumb.rank

            if thumb.external_id is not None:
                # We know about this thumb was in Brightcove so see if it
                # is still there.
                if thumb.external_id in bc_thumb_ids:
                    found_thumb = True
            elif thumb.refid is not None:
                # For legacy thumbs, we specified a reference id. Look for it
                if thumb.refid in bc_ref_ids:
                    found_thumb = True

                    yield tornado.gen.Task(neondata.ThumbnailMetadata.modify,
                                           thumb.key,
                                           _set_external_id)
            else:
                # We do not have the id for this thumb, so see if we
                # can match the url.
                norm_urls = set([normalize_url(x) for x in thumb.urls])
                if len(norm_urls.intersection(bc_urls)) > 0:
                    found_thumb = True
                    
                    # Now update the external id because we didn't
                    # know about it before.
                    yield tornado.gen.Task(neondata.ThumbnailMetadata.modify,
                                           thumb.key,
                                           _set_external_id)
                

        if not found_thumb:
            urls = get_urls_from_bc_response(data)
            added_image = False
            for url in urls[::-1]:
                # Add the new thumbnail to our system
                try:
                    new_thumb = neondata.ThumbnailMetadata(
                        None,
                        ttype=neondata.ThumbnailType.BRIGHTCOVE,
                        rank = min_rank-1,
                        external_id = external_id
                        )
                    yield vid_meta.download_and_add_thumbnail(
                        new_thumb,
                        url,
                        save_objects=True,
                        async=True)
            
                    _log.info(
                        'Found new thumbnail %s for video %s at Brigthcove.' %
                        (external_id, vid_meta.key))
                    statemon.state.increment('new_images_found')
                    added_image = True
                    break
                except IOError:
                    # Error getting the image, so keep going
                    pass
            if not added_image:
                _log.error('Could not find valid image to add to video %s. '
                           'Tried urls %s' % (vid_meta.key, urls))
                statemon.state.increment('cant_get_image')
            

@tornado.gen.coroutine
def run_one_cycle():
    platforms = yield tornado.gen.Task(
        neondata.BrightcovePlatform.get_all)
    yield [process_one_account(x) for x in platforms if x is not None]

@tornado.gen.coroutine
def main(run_flag):
    while run_flag.is_set():
        start_time = datetime.datetime.now()
        try:
            yield run_one_cycle()
            statemon.state.increment('cycles_complete')
        except Exception as e:
            _log.exception('Unexpected exception when ingesting from '
                           'Brightcove')
            statemon.state.increment('unexpected_exception')
        cycle_runtime = (datetime.datetime.now() - start_time).total_seconds()
        statemon.state.cycle_runtime = cycle_runtime

        if cycle_runtime < options.poll_period:
            yield tornado.gen.sleep(options.poll_period - cycle_runtime)
            
    _log.info('Finished program')

if __name__ == "__main__":
    utils.neon.InitNeon()
    run_flag = multiprocessing.Event()
    run_flag.set()
    atexit.register(utils.ps.shutdown_children)
    atexit.register(run_flag.clear)
    signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
    
    tornado.ioloop.IOLoop.current().run_sync(lambda: main(run_flag))
