'''
Brightcove API Interface class
'''

import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import datetime
import json
from poster.encode import multipart_encode
import poster.encode
from PIL import Image
import re
from StringIO import StringIO
import cmsdb.neondata 
import math
import time
import tornado.gen
import tornado.httpclient
import tornado.httputil
import tornado.ioloop
import tornado.escape
import urllib
import utils.http
import utils.logs
import utils.neon
from utils.http import RequestPool
from cvutils.imageutils import PILImageUtils

import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define('max_write_connections', default=1, type=int, 
       help='Maximum number of write connections to Brightcove')
define('max_read_connections', default=20, type=int, 
       help='Maximum number of read connections to Brightcove')
define('max_retries', default=5, type=int,
       help='Maximum number of retries when sending a Brightcove error')

class BrightcoveApiError(IOError): pass
class BrightcoveApiClientError(BrightcoveApiError): pass
class BrightcoveApiServerError(BrightcoveApiError): pass

DEFAULT_IMAGES_SIZES = {
    'thumbnail' : (120, 90),
    'poster' : (480, 360)
}

class BrightcoveApi(object): 

    ''' Brighcove API Interface class
    All video ids used in the class refer to the Brightcove platform VIDEO ID
    '''

    write_connection = RequestPool(options.max_write_connections,
                                   limit_for_subprocs=True)
    read_connection = RequestPool(options.max_read_connections)
    READ_URL = "http://api.brightcove.com/services/library"
    WRITE_URL = "http://api.brightcove.com/services/post"
    
    def __init__(self, neon_api_key, publisher_id=0, read_token=None,
                 write_token=None):
        self.publisher_id = publisher_id
        self.neon_api_key = neon_api_key
        self.read_token = read_token
        self.write_token = write_token

        self.THUMB_SIZE = 120, 90
        self.STILL_SIZE = 480, 360

    ###### Brightcove media api update method ##########

    @tornado.gen.coroutine
    def add_image(self, video_id, tid, image=None, remote_url=None,
                  atype='thumbnail', reference_id=None):
        '''Add Image brightcove api helper method
        
        #NOTE: When uploading an image with a reference ID that already
        #exists, the image is not
        updated. Although other metadata like displayName is updated.

        Inputs:
        video_id - Brightcove video id
        tid - Internal Neon thumbnail id for the image
        image - The image to upload
        remote_url - The remote url to set for this image
        atype - Type of image being uploaded. Either "thumbnail" or "videostill"
        reference_id - Reference id for the image to send to brightcove

        returns:
        dictionary of the JSON of the brightcove response
        '''
        #help.brightcove.com/developer/docs/mediaapi/add_image.cfm
        
        if reference_id:
            reference_id = "v2_%s" %reference_id #V2 ref ID

        im = image            
        image_fname = 'neontn%s.jpg' % (tid) 

        outer = {}
        params = {}
        params["token"] = self.write_token 
        params["video_id"] = video_id
        params["filename"] = image_fname
        params["resize"] = False
        image = {} 
        if reference_id is not None:
            image["referenceId"] = reference_id
        
        if atype == 'thumbnail':    
            image["type"] = "THUMBNAIL"
            image["displayName"] = str(self.publisher_id) + \
                    'neon-thumbnail-for-video-%s'%video_id
        else:
            image["type"] = "VIDEO_STILL"
            image["displayName"] = str(self.publisher_id) + \
                    'neon-video-still-for-video-%s'%video_id 
        
        if remote_url:
            image["remoteUrl"] = remote_url

        params["image"] = image
        outer["params"] = params
        outer["method"] = "add_image"

        body = tornado.escape.json_encode(outer)

        if remote_url:
            post_param = []
            args = poster.encode.MultipartParam("JSONRPC", value=body)
            post_param.append(args)
            datagen, headers = multipart_encode(post_param)
            body = "".join([data for data in datagen])

        else:
            #save image
            filestream = StringIO()
            im.save(filestream, 'jpeg')
            filestream.seek(0)
            image_data = filestream.getvalue()
            post_param = []
            fileparam = poster.encode.MultipartParam(
                "filePath",
                value=image_data,
                filetype='image/jpeg',
                filename=image_fname)
            args = poster.encode.MultipartParam("JSONRPC", value=body)
            post_param.append(args)
            post_param.append(fileparam)
            datagen, headers = multipart_encode(post_param)
            body = "".join([data for data in datagen])
        
        #send request
        req = tornado.httpclient.HTTPRequest(url=BrightcoveApi.WRITE_URL,
                                             method="POST",
                                             headers=headers, 
                                             body=body,
                                             request_timeout=60.0,
                                             connect_timeout=10.0)
        response = yield BrightcoveApi.write_connection.send_request(
            req, async=True, ntries=options.max_retries)
        if response.error:
            if response.error.code >= 500:
                raise BrightcoveApiServerError(
                    'Internal Brightcove error when uploading %s for tid %s %s'
                    % (atype, tid, response.error))
            elif response.error.code >= 400:
                raise BrightcoveApiClientError(
                    'Client error when uploading %s for tid %s %s'
                    % (atype, tid, response.error))
            raise BrightcoveApiClientError(
                'Unexpected error when uploading %s for tid %s %s'
                % (atype, tid, response.error))

        try:
            json_response = tornado.escape.json_decode(response.body)
        except Exception:
            raise BrightcoveApiServerError(
                'Invalid JSON received from Brightcove: %s' %
                response.body)

        raise tornado.gen.Return(json_response['result'])

    @tornado.gen.coroutine
    def update_thumbnail_and_videostill(self,
                                        video_id,
                                        tid,
                                        image=None,
                                        remote_url=None,
                                        thumb_size=None,
                                        still_size=None): 
    
        ''' add thumbnail and videostill in to brightcove account.  

        Inputs:
        video_id - brightcove video id
        tid - Thumbnail id to update reference id with
        image - PIL image to set the image with. Either this or remote_url
                must be set.
        remote_url - A remote url to push into brightcove that points to the
                     image
        thumb_size - (width, height) of the thumbnail
        still_size - (width, height) of the video still image

        Returns:
        (brightcove_thumb_id, brightcove_still_id)
        '''
        thumb_size = thumb_size or self.THUMB_SIZE
        still_size = still_size or self.STILL_SIZE
        if image is not None:
            # Upload an image and set it as the thumbnail
            thumb = PILImageUtils.resize(image,
                                         im_w=thumb_size[0],
                                         im_h=thumb_size[1])
            still = PILImageUtils.resize(image,
                                         im_w=still_size[0],
                                         im_h=still_size[1])

            responses = yield [self.add_image(video_id,
                                              tid,
                                              image=thumb,
                                              atype='thumbnail',
                                              reference_id=tid),
                               self.add_image(video_id,
                                              tid,
                                              image=still,
                                              atype='videostill',
                                              reference_id='still-%s'%tid)]
            raise tornado.gen.Return([x['id'] for x in responses])
        
        elif remote_url is not None:
            # Set the thumbnail as a remote url. If it is a neon
            # serving url, then add the requested size to the url
            thumb_url, is_thumb_neon_url = self._build_remote_url(
                remote_url, thumb_size)
            thumb_reference_id = tid
            if is_thumb_neon_url:
                thumb_reference_id = 'thumbservingurl-%s' % video_id
                
            still_url, is_still_neon_url = self._build_remote_url(
                remote_url, still_size)
            still_reference_id = tid
            if is_still_neon_url:
                still_reference_id = 'stillservingurl-%s' % video_id

            responses = yield [self.add_image(video_id,
                                              tid,
                                              remote_url=thumb_url,
                                              atype='thumbnail',
                                              reference_id=thumb_reference_id),
                               self.add_image(video_id,
                                              tid,
                                              remote_url=still_url,
                                              atype='videostill',
                                              reference_id=still_reference_id)]
            
            raise tornado.gen.Return([x['id'] for x in responses])

        else:
            raise TypeError('Either image or remote_url must be set')

    def _build_remote_url(self, url_base, size):
        '''Create a remote url. 

        If the base is a neon serving url, tack on the size params.

        returns (remote_url, is_neon_serving)
        '''
        remote_url = url_base
        neon_url_re = re.compile('/neonvid_[0-9a-zA-Z_\.]+$')
        is_neon_serving = neon_url_re.search(url_base) is not None
        if is_neon_serving:
            arams = zip(('width', 'height'), size)
            param_str = '&'.join(['%s=%i' % x for x in params if x[1]])
            if params_str:
                remote_url = '%s?%s' % (url_base, params_str)

        return remote_url, is_neon_serving

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_current_thumbnail_url(self, video_id):
        '''Method to retrieve the current thumbnail url on Brightcove
        
        Used by AB Test to keep track of any uploaded image to
        brightcove, its URL

        Inputs:
        video_id - The brightcove video id to get the urls for

        Returns thumb_url, still_url

        If there is an error, (None, None) is returned
        
        '''
        thumb_url = None
        still_url = None

        url = ('http://api.brightcove.com/services/library?' 
                'command=find_video_by_id&token=%s&media_delivery=http'
                '&output=json&video_id=%s'
                '&video_fields=videoStillURL%%2CthumbnailURL' %
                (self.read_token, video_id))

        req = tornado.httpclient.HTTPRequest(url=url,
                                             method="GET", 
                                             request_timeout=60.0,
                                             connect_timeout=10.0)
        response = yield BrightcoveApi.read_connection.send_request(
            req, ntries=options.max_retries, async=True)

        if response.error:
            _log.error('key=get_current_thumbnail_url '
                       'msg=Error getting thumbnail for video id %s'%video_id)
            raise tornado.gen.Return((None, None))

        try:
            result = tornado.escape.json_decode(response.body)
            thumb_url = result['thumbnailURL'].split('?')[0]
            still_url = result['videoStillURL'].split('?')[0]
        except ValueError as e:
            _log.error('key=get_current_thumbnail_url '
                       'msg=Invalid JSON response from %s' % url)
            raise tornado.gen.Return((None, None))
        except KeyError:
            _log.error('key=get_current_thumbnail_url '
                       'msg=No valid url set for video id %s' % video_id)
            raise tornado.gen.Return((None, None))

        raise tornado.gen.Return((thumb_url, still_url))

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def find_videos_by_ids(self, video_ids, video_fields=None,
                           custom_fields=None,
                           media_delivery='http'):
        '''Finds many videos by brightcove id.
        
        Inputs:
        video_ids - list of brightcove video ids to get info for
        video_fields - list of video fields to populate
        custom_fields - list of custom fields to populate in the result
        media_delivery - should urls be http, http_ios or default

        Outputs:
        A list of video objects. Each object will have the ['id'] field
        '''
        results = []

        MAX_VIDS_PER_REQUEST = 50
        
        for i in range(0, len(video_ids), MAX_VIDS_PER_REQUEST):
            url_params = {
                'command' : 'find_videos_by_ids',
                'token' : self.read_token,
                'video_ids' : ','.join(video_ids[i:(i+MAX_VIDS_PER_REQUEST)]),
                'media_delivery' : media_delivery,
                'output' : 'json'
                }
            if video_fields is not None:
                video_fields.append('id')
                url_params['video_fields'] = ','.join(set(video_fields))

            if custom_fields is not None:
                url_params['custom_fields'] = ','.join(set(custom_fields))

            request = tornado.httpclient.HTTPRequest(
                '%s?%s' % (BrightcoveApi.READ_URL,
                           urllib.urlencode(url_params)),
                request_timeout = 60.0)
            
            response = yield BrightcoveApi.read_connection.send_request(
                request, ntries=options.max_retries, async=True)

            results.extend(_handle_response(response))

        raise tornado.gen.Return(results)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def search_videos(self, _all=None, _any=None, none=None, sort_by=None,
                      exact=False, page_size=100,
                      video_fields=None, custom_fields=None, 
                      media_delivery='http', page=None):
        '''Search for videos based on some criteria.

        For more details on the call, see
        http://docs.brightcove.com/en/video-cloud/media/guides/search_videos-guide.html

        Inputs:
        _all - list of (field,value) pairs that MUST be present to be returned
        _any - list of (field,value) pairs that AT LEAST ONE must be present
        none - list of (field,value) pairs that MUST NOT be present
        sort_by - field to sort by and direction. e.g. PUBLISH_DATE:DESC
        exact - If true requires exact match of search terms
        page_size - Number of pages to grab on each call
        video_fields - list of video fields to populate
        custom_fields - list of custom fields to populate in the result
        media_delivery - should urls be http, http_ios or default
        page - page to return. If set, only one page of results will be
               returned

        Outputs:
        List of video objects, which are dictionaries with the
        requested fields filled out.
        '''
        # Build the request
        url_params = {
            'command' : 'search_videos',
            'token' : self.read_token,
            'output' : 'json',
            'media_delivery' : media_delivery,
            'page_number' : 0 if page is None else page,
            'page_size' : page_size,
            'cache_buster' : time.time()
            }
        if _all is not None:
            url_params['all'] = ','.join(
                ['%s:%s' % x if x[0] is not None else str(x[1]) 
                 for x in _all])

        if _any is not None:
            url_params['any'] = ','.join(
                ['%s:%s' % x if x[0] is not None else str(x[1]) 
                 for x in _any])

        if none is not None:
            url_params['none'] = ','.join(
                ['%s:%s' % x if x[0] is not None else str(x[1]) 
                 for x in none])

        if sort_by is not None:
            url_params['sort_by'] = sort_by

        if exact:
            url_params['exact'] = 'true'


        if video_fields is not None:
            video_fields.append('id')
            url_params['video_fields'] = ','.join(set(video_fields))

        if custom_fields is not None:
            url_params['custom_fields'] = ','.join(set(custom_fields))

        request = tornado.httpclient.HTTPRequest(
            '%s?%s' % (BrightcoveApi.READ_URL, urllib.urlencode(url_params)),
            decompress_response=True,
            request_timeout = 120.0)

        response = yield BrightcoveApi.read_connection.send_request(
            request, ntries=options.max_retries, async=True)

        results = _handle_response(response)

        raise tornado.gen.Return(results)

    def search_videos_iter(self, page_size=100, max_results=None, **kwargs):
        '''Shortcut to return an iterator for a search_videos call.

        Uses all the same arguments as search_videos
        '''
        return BrightcoveFeedIterator(self.search_videos, page_size,
                                      max_results, **kwargs)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def find_modified_videos(self, from_date=datetime.datetime(1970,1,1),
                             _filter=None,
                             page_size=100,
                             page=0,
                             sort_by=None,
                             sort_order='DESC',
                             video_fields=None,
                             custom_fields=None,
                             media_delivery='http'):
        '''Find all the videos that have been modified since a given time.

        Inputs:
        from_date - A datetime object for when to get videos
        _filter - List of categories that should be returned. 
                  Can be PLAYABLE, UNSCHEDULED, INACTIVE, DELETED
        page_size - Max page size
        page - The page number to get
        sort_by - field to sort by. e.g. MODIFIED_DATE
        sort_order - Direction to sort by ASC or DESC
        video_fields - list of video fields to populate
        custom_fields - list of custom fields to populate in the result
        media_delivery - should urls be http, http_ios or default

        Returns:
        list of video objects
        '''
        from_date_mins = int(math.floor(
            (from_date - datetime.datetime(1970, 1, 1)).total_seconds() / 
            60.0))
        
        # Build the request
        url_params = {
            'command' : 'find_modified_videos',
            'token' : self.read_token,
            'output' : 'json',
            'media_delivery' : media_delivery,
            'page_number' : page,
            'page_size' : page_size,
            'sort_order' : sort_order,
            'from_date' : from_date_mins,
            'cache_buster' : time.time()
            }

        if _filter is not None:
            url_params['filter'] = ','.join(_filter)

        if sort_by is not None:
            url_params['sort_by'] = sort_by

        if video_fields is not None:
            video_fields.append('id')
            url_params['video_fields'] = ','.join(set(video_fields))

        if custom_fields is not None:
            url_params['custom_fields'] = ','.join(set(custom_fields))

        request = tornado.httpclient.HTTPRequest(
            '%s?%s' % (BrightcoveApi.READ_URL, urllib.urlencode(url_params)),
            decompress_response=True,
            request_timeout = 120.0)

        response = yield BrightcoveApi.read_connection.send_request(
            request, async=True, ntries=options.max_retries)

        results = _handle_response(response)

        raise tornado.gen.Return(results)

    def find_modified_videos_iter(self, page_size=100, max_results=None, **kwargs):
        '''Shortcut to return an iterator for a find_modified_videos.

        Uses all the same arguments as search_videos
        '''
        return BrightcoveFeedIterator(self.find_modified_videos, page_size,
                                      max_results, **kwargs)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def find_playlist_by_id(self, playlist_id, video_fields=None,
                            playlist_fields=None, custom_fields=None,
                            media_delivery='http'):
        '''Gets a playlist by id.

        playlist_id - Id of the playlist to return
        video_fields - list of video fields to populate
        playlist_fields - list of playlist fields to populate
        custom_fields - list of custom fields to populate in the result
        media_delivery - should urls be http, http_ios or default

        Returns:
        A playlist object
        '''
        # Build the request
        url_params = {
            'command' : 'find_playlist_by_id',
            'token' : self.read_token,
            'output' : 'json',
            'media_delivery' : media_delivery,
            'playlist_id': playlist_id,
            'cache_buster' : time.time()
            }

        if playlist_fields is not None:
            playlist_fields.append('videos')
            url_params['playlist_fields'] = ','.join(set(playlist_fields))

        if video_fields is not None:
            video_fields.append('id')
            url_params['video_fields'] = ','.join(set(video_fields))

        if custom_fields is not None:
            url_params['custom_fields'] = ','.join(set(custom_fields))

        request = tornado.httpclient.HTTPRequest(
            '%s?%s' % (BrightcoveApi.READ_URL, urllib.urlencode(url_params)),
            decompress_response=True,
            request_timeout = 120.0)

        response = yield BrightcoveApi.read_connection.send_request(
            request, async=True, ntries=options.max_retries)

        results = _handle_response(response)

        raise tornado.gen.Return(results)
        

def _handle_response(response):
    '''Handles the response from Brightcove

    Returns: a list of video objects
    '''
    if response.error:
        _log.error('Error getting video info from brightcove: %s' %
                   response.body)
        try:
            json_data = json.load(response.buffer)
            if json_data['code'] >= 200:
                raise BrightcoveApiClientError(json_data)
        except ValueError:
            # It's not JSON data so there was some other error
            pass    
        except KeyError:
            # It may be valid json but doesn't have a code
            pass
        raise BrightcoveApiServerError('Error: %s JSON: %s' %
                                       (response.error, response.buffer))

    json_data = json.load(response.buffer)
    return [x for x in json_data['items'] if x is not None]

class BrightcoveFeedIterator(object):
    '''An iterator that walks through entries from a Brightcove feed.

    Should be used to wrap a function from BrightcoveApi. e.g.
    BrightcoveFeedIterator(api.search_videos, page_size=100, max_results=1000,
                           **kwargs)

    Automatically deals with paging.

    If you want to do this iteration so that any calls are
    asynchronous, then you have to manually create a loop like:

    while True:
       item = yield iter.next(async=True)
       if isinstance(item, StopIteration):
          break

    Note, in the asynchronous case, you have to catch the special
    api.brightcove_api.FeedStopIteration because StopIteration is
    dealt with specially by the tornado framework
    
    '''
    def __init__(self, func, page_size=100, max_results=None, **kwargs):
        '''Create an iterator

        Inputs:
        func - Function to call to get a single page of results
        page_size - The size of each page when it is requested
        max_results - The maximum number of entries to return
        kwargs - Any other arguments to pass to func
        '''
        self.func = func
        self.args = kwargs
        self.args['page_size'] = min(page_size, 100)
        self.args['page'] = 0
        self.max_results = max_results
        self.page_data = []
        self.items_returned = 0

    def __iter__(self):
        self.args['page'] = 0
        self.items_returned = 0
        return self

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def next(self):
        if (self.max_results is not None and 
            self.items_returned >= self.max_results):
            e = StopIteration()
            e.value = StopIteration()
            raise e
        
        if len(self.page_data) == 0:
            # Get more entries
            self.page_data = yield self.func(async=True, **self.args)
            self.page_data.reverse()
            self.args['page'] += 1

        if len(self.page_data) == 0:
            # We've gotten all the data
            e = StopIteration()
            e.value = StopIteration()
            raise e

        self.items_returned += 1
        raise tornado.gen.Return(self.page_data.pop())
