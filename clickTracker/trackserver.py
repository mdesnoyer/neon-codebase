#!/usr/bin/env python
''''
Server that logs data from the tracker and sends it to a local flume agent

Tornado server listens for http requests and sends them to flume. If
flume can't handle the load, the events are logged to disk and then
replayed when flume comes back.

'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import avro.io
import avro.schema
from clickTracker.flume import ThriftSourceProtocol
from clickTracker.flume.ttypes import *
from clickTracker import TTornado
import hashlib
import httpagentparser
import json
import os
import Queue
import re
import shortuuid
import socket
from cStringIO import StringIO
import threading
from thrift import Thrift
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol
import time
import tornado.gen
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.escape
import urllib2
import utils.http
from utils.inputsanitizer import InputSanitizer
import utils.logs
import utils.neon
import utils.ps
import utils.sync

import boto.exception
from utils.s3 import S3Connection

#logging
import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define("port", default=9080, help="run on the given port", type=int)
define("flume_port", default=6367, type=int,
       help='Port to talk to the flume agent running locally')
define("backup_disk", default="/mnt/neon/backlog", type=str,
        help="Location to store backup lines which failed to send to the flume agent")
define("backup_max_events_per_file", default=100000, type=int,
       help='Maximum events to allow backups on per file')
define("flume_flush_interval", default=100, type=int,
       help='Flush flume events after how many events?')
define("message_schema",
       default=os.path.abspath(
           os.path.join(os.path.dirname(__file__), '..', 'schema',
                        'compiled', 'TrackerEvent.avsc')),
        help='Path to the output avro message schema (avsc) file')
define("schema_bucket", default="neon-avro-schema",
       help='S3 Bucket that contains schemas')
define("isp_host", default="127.0.0.1",
       help="Host where the image serving platform is.")
define("isp_port", default=8089,
       help="Host where the image serving platform resides")
define('loggly_base_url',
       default='https://logs-01.loggly.com/inputs/520b9697-b7f3-4970-a059-710c28a8188a',
       help='Base url for the loggly endpoint')

from utils import statemon
statemon.define('qsize', int)
statemon.define('flume_errors', int)
statemon.define('messages_handled', int)
statemon.define('invalid_messages', int)
statemon.define('internal_server_error', int)
statemon.define('unknown_basename', int)
statemon.define('malformed_basename', int)
_malformed_basename_ref = statemon.state.get_ref('malformed_basename')
statemon.define('isp_connection_error', int)
statemon.define('not_interesting_message', int)
statemon.define('invalid_video_id', int)
statemon.define('invalid_thumbnails', int)
_invalid_thumbnails_ref = statemon.state.get_ref('invalid_thumbnails')

class NotInterestingData(Exception): pass

#############################################
#### DATA FORMAT ###
#############################################

class BaseTrackerDataV2(object):
    '''
    Object that mirrors the the Avro TrackerEvent schema and is used to 
    write the Avro data
    '''
    # A map from schema entries to the http headers where the value is found
    header_map = {
        'uagent' : 'User-Agent',
        'country': 'Geoip_country_code3',
        'city' : 'Geoip_city',
        'region' : "Geoip_region",
        'zip' : "Geoip_postal_code",
        'lat' : "Geoip_latitude",
        'lon' : "Geoip_longitude"
        }

    tracker_type_map = {
        'brightcove' : 'BRIGHTCOVE',
        'ooyala' : 'OOYALA',
        'bcgallery' : 'BCGALLERY',
        'ign' : 'IGN',
        'gen' : 'GENERAL'
        }
        
    
    def __init__(self, request, isp_host, isp_port):
        self.isp_host = isp_host
        self.isp_port = isp_port
        
        self.pageId = request.get_argument('pageid') # page_id
        self.trackerAccountId = request.get_argument('tai') # tracker_account_id
        # tracker_type (brightcove, ooyala, bcgallery, ign as of April 2014)
        try:
            self.trackerType = \
              BaseTrackerDataV2.tracker_type_map[request.get_argument('ttype').lower()]
        except KeyError:
            msg = "Invalid ttype %s" % request.get_argument('ttype')
            _log.error(msg)
            raise tornado.web.HTTPError(400, reason=msg)
        
        self.pageURL = request.get_argument('page') # page_url
        self.refURL = request.get_argument('ref', None) # referral_url

        self.serverTime = long(time.time() * 1000) # Server time stamp in ms
        self.clientTime = long(request.get_argument('cts')) # client_time in ms
        self.clientIP = request.request.remote_ip # client_ip
        # Neon's user id
        self.neonUserId = request.get_cookie('neonglobaluserid', default="") 

        self.userAgent = self.get_header_safe(request, 'User-Agent')
        if self.userAgent:
            self.agentInfo = BaseTrackerDataV2.extract_agent_info(
                self.userAgent)

        self.ipGeoData = {
            'country': self.get_header_safe(request, 'Geoip_country_code3'),
            'city': self.get_header_safe(request, 'Geoip_city'),
            'region': self.get_header_safe(request, 'Geoip_region'),
            'zip': self.get_header_safe(request, 'Geoip_postal_code'),
            'lat': self.get_header_safe(request, 'Geoip_latitude', float),
            'lon': self.get_header_safe(request, 'Geoip_longitude', float)
            }

        self.eventData = {}

    @tornado.gen.coroutine
    def fill_thumbnail_ids(self, request):
        '''Fills the thumbnail ids for the event.

        Must be implemented by subclasses if necessary, but defaults
        to filling a single thumbnail id with the 'tid' or 'bn'
        argument that is optional (if it is not there, the thumbnail
        id is unknown.
        '''
        self.eventData['thumbnailId'] = None
        try:
            # Try getting the thumbnail id when it is explicit in the arguments
            self.eventData['thumbnailId'] = \
              InputSanitizer.sanitize_null(request.get_argument('tid'))
        except tornado.web.MissingArgumentError:
            # Now try getting it from the image basename
            try:
                bn = request.get_argument('bn')
            except tornado.web.MissingArgumentError:
                # It's optional, so stop
                return
            tids = yield self._lookup_thumbnail_ids_from_isp([bn])
            if tids[0] is None:
                raise NotInterestingData()
            self.eventData['thumbnailId'] = InputSanitizer.sanitize_null(
                tids[0])
        self.eventData['thumbnailId'] = self.validate_thumbnail_ids(
            [self.eventData['thumbnailId']])[0]

    def validate_thumbnail_ids(self, tids):
        '''Replaces thumbnail id by None if it is not valid.

        Inputs:
        tids - List of tids

        Returns:
        list of valid tids, or None if it was invalid
        '''
        # TODO(mdesnoyer): Remove the split by dashes once the
        # brightcove tracker code is fixed. It should just be
        # underscores.
        tidRe = re.compile('^[0-9a-zA-Z]+[\-_][0-9a-zA-Z\-~\.]+[\-_][0-9a-zA-Z]+$')
        tidRe = re.compile('^[0-9a-zA-Z]+_[0-9a-zA-Z\-~\.]+_[0-9a-zA-Z]+$')
        dashTidRe = re.compile('^[0-9a-zA-Z]+\-[0-9a-zA-Z~\.]+\-[0-9a-zA-Z]+$')
        retval = []
        for tid in tids:
            if tid is None:
                retval.append(tid)
            elif tidRe.match(tid):
                retval.append(tid)
            elif dashTidRe.match(tid):
                # Replace the dashes with underscores
                retval.append(re.sub('\-', '_', tid))
            else:
                retval.append(None)
        return retval

    def validate_video_id(self, vid):
        '''Returns the video id or None if it is invalid

        Inputs:
        vid - Video id to validate

        Returns:
        valid video id, or raises tornado.web.MissingArgumentError
        '''
        vidRe = re.compile('^[0-9a-zA-Z~\-\.]+$')
        if vid is None or vidRe.match(vid):
            return vid
        _log.warn_n("Video %s is not interesting" % vid, 100)
        statemon.state.increment('invalid_video_id')
        raise NotInterestingData()

    @tornado.gen.coroutine
    def _lookup_thumbnail_ids_from_isp(self, basenames):
        '''Uses the image serving platform to find the thumbnails ids.

        Inputs:
        basenames - List of image basenames

        Returns:
        list of thumbnail ids, or None if it is unknown
        '''
        vidRe = re.compile('neonvid_([0-9a-zA-Z\-~\.]+)(\.jpg)?')
        vidReJpg = re.compile('neonvid_([0-9a-zA-Z\-~\.]+)\.jpg')
        # TODO(mdesnoyer): Remove the split by dashes once the
        # brightcove tracker code is fixed. It should just be
        # underscores.
        tidRe = re.compile('neontn([0-9a-zA-Z]+_[0-9a-zA-Z\-~\.]+_[0-9a-zA-Z]+)')
        dashTidRe = re.compile('neontn([0-9a-zA-Z]+\-[0-9a-zA-Z~\.]+\-[0-9a-zA-Z]+)')

        # Parse the basenames
        vids = []
        tids = []
        for bn in basenames:
            tidSearch = tidRe.search(bn)
            dashSearch = dashTidRe.search(bn)
            if tidSearch:
                tids.append(tidSearch.group(1))
                vids.append(None)
            elif dashSearch:
                tids.append(re.sub('\-', '_', dashSearch.group(1)))
                vids.append(None)
            else:
                vidSearch = vidRe.search(bn)
                vidSearchJpg = vidReJpg.search(bn)
                tids.append(None)
                if vidSearchJpg:
                    vids.append(vidSearchJpg.group(1))
                elif vidSearch:
                    vids.append(vidSearch.group(1))
                else:
                    _log.warn_n('Malformed basename %s' % bn, 100)
                    vids.append(None)
                    statemon.state.increment(ref=_malformed_basename_ref,
                                             safe=False)

        # Send a request to the image serving platform for all the video ids
        to_req =  [x for x in vids if x is not None]
        if len(to_req) > 0:
            headers = ({"Cookie" : 'neonglobaluserid=%s' % self.neonUserId} 
                       if self.neonUserId else {})
            # GetThumbnailId uses xfr if userId is not ready to be tested
            # to determine the abtest bucket
            if self.clientIP:
                headers["X-Forwarded-For"] = self.clientIP 

            request = tornado.httpclient.HTTPRequest(
                'http://%s:%s/v1/getthumbnailid/%s?params=%s' % (
                    self.isp_host,
                    self.isp_port,
                    self.trackerAccountId,
                    ','.join(to_req)),
                headers=headers)
            response = yield tornado.gen.Task(utils.http.send_request, request)
            if response.error:
                    
                statemon.state.increment('isp_connection_error')
                _log.error('Error getting tids from the image serving '
                           'platform.')
                raise tornado.web.HTTPError(500, str(response.error))
            
            tid_response = response.body.split(',')
            if len(tid_response) != len(to_req):
                _log.error('Response from the Image Serving Platform is '
                           'invalid. Request was %s. Response was %s' % 
                           (request.url, response.body))
                raise tornado.web.HTTPError(500)
            responseI = 0
            for i in range(len(vids)):
                if vids[i] is None:
                    # we didn't request this entry
                    continue
                elif tid_response[responseI] == 'null':
                    statemon.state.increment('unknown_basename')
                    _log.error_n('No thumbnail id known for video id %s' %
                                 to_req[responseI], 10)
                else:
                    tids[i] = tid_response[responseI]
                responseI += 1

        raise tornado.gen.Return(tids)

    def get_header_safe(self, request, header_name, typ=unicode):
        '''Returns the header value, or None if it's not there.'''
        try:
            strval = unicode(request.request.headers[header_name], 'utf-8')
            if strval == '':
                return None
            return typ(strval)
        except KeyError:
            return None
        except ValueError as e:
            raise tornado.web.HTTPError(
                400, "Invalid header info %s" % e)
            

    @staticmethod
    def extract_agent_info(uagent):
        retval = {}
        try:
            raw_data = httpagentparser.detect(uagent)
            if 'browser' not in raw_data:
                return None
            retval['browser'] = raw_data['browser']
            if 'dist' in raw_data:
                retval['os'] = raw_data['dist']
            elif 'flavor' in raw_data:
                retval['os'] = raw_data['flavor']
            elif 'platform' in raw_data:
                retval['os'] = raw_data['platform']
            else:
                retval['os'] = raw_data['os']
        except Exception, e:
            _log.exception("httpagentparser failed %s" % e)
            return None
        return retval

    def to_flume_event(self, writer, schema_url):
        '''Coverts the data to a flume event.'''
        encoded_str = StringIO()
        encoder = avro.io.BinaryEncoder(encoded_str)
        writer.write(self.__dict__, encoder)
        return ThriftFlumeEvent(headers = {
                'timestamp' : str(self.serverTime),
                'tai' : self.trackerAccountId,
                'track_vers' : '3.0',
                'event' : self.eventType,
                'flume.avro.schema.url' : schema_url
                }, body=encoded_str.getvalue())

    @staticmethod
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def generate(request_handler, isp_host=options.isp_host,
                 isp_port=options.isp_port):
        '''A Factory generator to make the event.

        Inputs:
        request_handler - The http request handler
        '''
        event_map = {
            'iv' : ImagesVisible,
            'il' : ImagesLoaded,
            'ic' : ImageClicked,
            'vp' : VideoPlay,
            'vc' : VideoClick,
            'ap' : AdPlay,
            'vvp' : VideoViewPercentage}

        action = request_handler.get_argument('a')
        try:
            event = event_map[action](request_handler, isp_host, isp_port)
            yield event.fill_thumbnail_ids(request_handler)
            raise tornado.gen.Return(event)
        except KeyError as e:
            msg = 'Invalid event: %s' % action
            _log.error(msg)
            raise tornado.web.HTTPError(400, reason=msg)
    
class ImagesVisible(BaseTrackerDataV2):
    '''An event specifying that the image became visible.'''
    def __init__(self, request, isp_host, isp_port):
        super(ImagesVisible, self).__init__(request, isp_host, isp_port)
        self.eventData['isImagesVisible'] = True
        self.eventType = 'IMAGES_VISIBLE'
        self.eventData['thumbnailIds'] = []

    @tornado.gen.coroutine
    def fill_thumbnail_ids(self, request):
        try:
            tids = request.get_argument('tids').split(',')
        except tornado.web.MissingArgumentError:
            tids = yield self._lookup_thumbnail_ids_from_isp(
                request.get_argument('bns').split(','))
        vtids = self.validate_thumbnail_ids(tids)
        self.eventData['thumbnailIds'] = [x for x in vtids if x is not None]
        if len(self.eventData['thumbnailIds']) == 0:
            _log.warn_n("No valid thumbnail ids in %s" % tids, 100)
            statemon.state.increment(ref=_invalid_thumbnails_ref,
                                     safe=False)
            raise NotInterestingData()

class ImagesLoaded(BaseTrackerDataV2):
    '''An event specifying that the image were loaded.'''
    def __init__(self, request, isp_host, isp_port):
        super(ImagesLoaded, self).__init__(request, isp_host, isp_port)
        self.eventData['isImagesLoaded'] = True
        self.eventType = 'IMAGES_LOADED'
        self.eventData['images'] = [] 

    @tornado.gen.coroutine
    def fill_thumbnail_ids(self, request):
        tids = []
        vids = []
        widths = []
        heights = []
        images = []
        has_tids = False
        try:
            arg_list = request.get_argument('tids')
            has_tids = True
        except tornado.web.MissingArgumentError:
            arg_list = request.get_argument('bns')
        if len(arg_list) > 0:
            for tup in arg_list.split(','):
                elems = tup.split(' ') # '+' delimiter converts to ' '
                if len(elems) != 3:
                    msg = ("tuple of (tid,width,height) is needed but "
                           "found: %s" % elems)
                    raise tornado.web.MissingArgumentError(msg)
                if has_tids:
                    tids.append(elems[0])
                else:
                    vids.append(elems[0])
                try:
                    widths.append(int(float(elems[1])))
                    heights.append(int(float(elems[2])))
                except ValueError:
                    raise tornado.web.MissingArgumentError(
                        'height and width must be ints. Saw: %s' % elems[1:])

            if not has_tids:
                tids = yield self._lookup_thumbnail_ids_from_isp(vids)
            tids = self.validate_thumbnail_ids(tids)

            for w, h, tid in zip(widths, heights, tids):
                if tid is not None:
                    images.append({'thumbnailId' : tid,
                                   'width' : w,
                                   'height' : h})
        self.eventData['images'] = images
        if len(self.eventData['images']) == 0:
            _log.warn_n("No interesting thumbnail ids in %s" % arg_list,
                        100)
            statemon.state.increment(ref=_invalid_thumbnails_ref,
                                     safe=False)
            raise NotInterestingData()
            

class ImageClicked(BaseTrackerDataV2):
    '''An event specifying that the image was clicked.'''
    def __init__(self, request, isp_host, isp_port):
        super(ImageClicked, self).__init__(request, isp_host, isp_port)
        self.eventData['isImageClick'] = True
        self.eventType = 'IMAGE_CLICK'
        self.eventData['thumbnailId'] = None
        self.eventData['pageCoords'] = {
            'x' : float(request.get_argument('x', 0)),
            'y' : float(request.get_argument('y', 0))
            }
        self.eventData['windowCoords'] = {
            'x' : float(request.get_argument('wx', 0)),
            'y' : float(request.get_argument('wy', 0))
            }
        self.eventData['imageCoords'] = {
            'x' : float(request.get_argument('cx', 0)),
            'y' : float(request.get_argument('cy', 0))
            }

    @tornado.gen.coroutine
    def fill_thumbnail_ids(self, request):
        '''The thumbnail id is required, so we can't use the default.'''
        try:
            tid = request.get_argument('tid')
        except tornado.web.MissingArgumentError:
            tids = yield self._lookup_thumbnail_ids_from_isp(
                [request.get_argument('bn')])
            tid = tids[0]
        self.eventData['thumbnailId'] = self.validate_thumbnail_ids([tid])[0]

        if self.eventData['thumbnailId'] is None:
            _log.warn_n("Invalid thumbnail id %s" % tid, 100)
            statemon.state.increment(ref=_invalid_thumbnails_ref,
                                     safe=False)
            raise NotInterestingData()

class VideoClick(BaseTrackerDataV2):
    '''An event specifying that the image was clicked within the player'''
    def __init__(self, request, isp_host, isp_port):
        super(VideoClick, self).__init__(request, isp_host, isp_port)
        self.eventData['isVideoClick'] = True
        self.eventData['thumbnailId'] = None
        self.eventType = 'VIDEO_CLICK'
         # External Video id
        self.eventData['videoId'] = self.validate_video_id(
            request.get_argument('vid'))
        self.eventData['playerId'] = request.get_argument('playerid', None) # Player id

class VideoPlay(BaseTrackerDataV2):
    '''An event specifying that the image were loaded.'''
    def __init__(self, request, isp_host, isp_port):
        super(VideoPlay, self).__init__(request, isp_host, isp_port)
        self.eventData['isVideoPlay'] = True
        
        self.eventType = 'VIDEO_PLAY'
        # Thumbnail id
        self.eventData['thumbnailId'] = None
         # External Video id
        self.eventData['videoId'] = self.validate_video_id(
            request.get_argument('vid'))
         # Player id
        self.eventData['playerId'] = request.get_argument('playerid', None)
         # If an adplay preceeded video play 
        self.eventData['didAdPlay'] = InputSanitizer.to_bool(
            request.get_argument('adplay', False))
        # (time when player initiates request to play video - 
        #             Last time an image or the player was clicked) 
        # autoplay delta in milliseconds
        self.eventData['autoplayDelta'] = InputSanitizer.sanitize_int(
            request.get_argument('adelta', None))
        self.eventData['playCount'] = InputSanitizer.sanitize_int(
            request.get_argument('pcount')) #the current count of the video playing on the page 
        self.eventData['isAutoPlay'] = InputSanitizer.to_bool(
            request.get_argument('aplay', None), is_null_valid=True)

        if (self.eventData['isAutoPlay'] is None and 
            request.get_argument('adelta', None) is None):
            raise tornado.web.MissingArgumentError(
                'either adelta or aplay must be present')

class AdPlay(BaseTrackerDataV2):
    '''An event specifying that the image were loaded.'''
    def __init__(self, request, isp_host, isp_port):
        super(AdPlay, self).__init__(request, isp_host, isp_port)
        self.eventData['isAdPlay'] = True
        
        self.eventType = 'AD_PLAY'
        # Thumbnail id
        self.eventData['thumbnailId'] = None
        #VID can be null, if VideoClick event doesn't fire before adPlay
        # Video id
        self.eventData['videoId'] = self.validate_video_id(
            InputSanitizer.sanitize_null(request.get_argument('vid'))) 
        # Player id
        self.eventData['playerId'] = request.get_argument('playerid', None)
        # (time when player initiates request to play video - Last
        # time an image or the player was clicked) autoplay delta in
        # millisecond
        self.eventData['autoplayDelta'] = InputSanitizer.sanitize_int(
            request.get_argument('adelta', None))
         #the current count of the video playing on the page
        self.eventData['playCount'] = InputSanitizer.sanitize_int(
            request.get_argument('pcount'))

        self.eventData['isAutoPlay'] = InputSanitizer.to_bool(
            request.get_argument('aplay', None), is_null_valid=True)

        if (self.eventData['isAutoPlay'] is None and 
            request.get_argument('adelta', None) is None):
            raise tornado.web.MissingArgumentError(
                'either adelta or aplay must be present')

class VideoViewPercentage(BaseTrackerDataV2):
    '''An event specifying that a percentage of the video was viewed.'''
    def __init__(self, request, isp_host, isp_port):
        super(VideoViewPercentage, self).__init__(request, isp_host, isp_port)
        self.eventData['isVideoViewPercentage'] = True

        self.eventType = 'VIDEO_VIEW_PERCENTAGE'

        # External video id
        self.eventData['videoId'] = self.validate_video_id(
            request.get_argument('vid'))

        #the current count of the video playing on the page
        self.eventData['playCount'] = InputSanitizer.sanitize_int(request.get_argument('pcount'))

        # Percentage of the video that has been seen (1-100)
        try:
            self.eventData['percent'] = round(
                float(request.get_argument('prcnt')))
        except ValueError:
            raise tornado.web.MissingArgumentError('prcnt')

    @tornado.gen.coroutine
    def fill_thumbnail_ids(self, request):
        '''There is no thumbnail id for this event, so just return.'''
        return

#############################################
#### WEB INTERFACE #####
#############################################

class TrackerDataHandler(tornado.web.RequestHandler):
    '''Common class to handle http requests to the tracker.'''
    def initialize(self):
        self.isp_host = options.isp_host
        self.isp_port = options.isp_port

    @tornado.gen.coroutine
    def parse_tracker_data(self, version):
        '''Parses the tracker data from a GET request.

        returns:
        TrackerData object
        '''
        if version == 2:
            event = yield BaseTrackerDataV2.generate(self,
                                                     isp_host=self.isp_host,
                                                     isp_port=self.isp_port,
                                                     async=True)
            raise tornado.gen.Return(event)
        else:
            _log.fatal('Invalid api version %s' % version)
            raise ValueError('Bad version %s' % version)

class FlumeBuffer:
    '''Class that handles buffering messages to flume.'''
    def __init__(self, port, backup_q):
        self.port = port
        self.backup_q = backup_q
        self.buffer = []
        self.flush_interval = options.flume_flush_interval
        
    @tornado.gen.coroutine
    def send(self, event):
        '''Send an events to flume.

        event - A ThriftFlumeEvent object
        '''
        self.buffer.append(event)

        if len(self.buffer) >= self.flush_interval:
            yield self._send_buffer()

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def flush(self):
        yield self._send_buffer()

    @tornado.gen.coroutine
    def _send_buffer(self):
        '''Sends all the events in the buffer to flume.'''
        # First copy the buffer and put a new empty one in so that
        # another call can add to it without losing messages.
        local_buf = self.buffer
        self.buffer = []

        # Open the connection to flume
        transport = TTornado.TTornadoStreamTransport('localhost', self.port)
        pfactory = TCompactProtocol.TCompactProtocolFactory()
        client = ThriftSourceProtocol.Client(transport, pfactory)
        try:
            yield tornado.gen.Task(transport.open)
        except TTransport.TTransportException as e:
            _log.error('Error opening connection to Flume: %s' % e)
            self._register_flume_error(local_buf)
            return

        # Send the data to flume
        try:            
            status = yield tornado.gen.Task(client.appendBatch, local_buf)
            if status != Status.OK:
                raise Thrift.TException('Flume returned error: %s' % status)
        except Thrift.TException as e:
            _log.error('Error writing to Flume: %s' % e)
            self._register_flume_error(local_buf)
            
        except IOError as e:
            _log.error('Error writing to Flume stream: %s' % e)
            self._register_flume_error(local_buf)

        finally:
            # Make sure we close the connection to avoid open sockets
            transport.close()

    def _register_flume_error(self, event_buf=[]):
        statemon.state.increment('flume_errors')
        for event in event_buf:
            self.backup_q.put(event)

class LogLines(TrackerDataHandler):
    '''Handler for real tracking data that should be logged.'''

    def initialize(self, watcher, version, avro_writer, schema_url,
                   flume_buffer):
        '''Initialize the logger.'''
        super(LogLines, self).initialize()
        self.watcher = watcher
        self.version = version
        self.avro_writer = avro_writer
        self.schema_url = schema_url
        self.flume_buffer = flume_buffer

        # We grab a ref to this message counter because it is used a
        # lot and the inspection can take a while. So do the
        # inspection now.
        self.message_counter = statemon.state.get_ref('messages_handled')
        self.not_interesting_counter = \
          statemon.state.get_ref('not_interesting_message')
        self.invalid_msg_counter = \
          statemon.state.get_ref('invalid_messages')
    
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self, *args, **kwargs):
        '''Handle a tracking request.'''
        with self.watcher.activate():
            statemon.state.increment(ref=self.message_counter, safe=False)
            
            try:
                tracker_data = yield self.parse_tracker_data(self.version)
            except tornado.web.MissingArgumentError as e:
                _log.error('Invalid request: %s' % self.request.uri)
                statemon.state.increment(ref=self.invalid_msg_counter,
                                         safe=False)
                if e.reason is None:
                    e.reason = e.log_message
                else:
                    e.reason = '%s: %s' % (e.reason, e.log_message)
                raise e
            except tornado.web.HTTPError as e:
                _log.error('Error processing request %s: %s' % (
                    self.request.uri, e))
                statemon.state.increment('internal_server_error')
                raise
            except NotInterestingData as e:
                # The data wasn't interesting to us even though it was
                # valid, so just record that and don't send the data
                # on.
                statemon.state.increment(ref=self.not_interesting_counter,
                                         safe=False)
                self.set_status(200)
                self.add_header("content-type", "application/javascript") 
                self.finish()
                return
            except Exception, err:
                _log.exception("Unexpected error parsing %s: %s",
                               self.request.uri, err)
                statemon.state.increment('internal_server_error')
                self.set_status(500)
                self.finish()
                return

            data = tracker_data.to_flume_event(self.avro_writer,
                                               self.schema_url)
            try:
                yield self.flume_buffer.send(data)
                self.add_header("content-type", "application/javascript") 
                self.set_status(200)
                
            except Exception, err:
                _log.exception("Unexpected error sending data to flume: %s",
                               err)
                self.set_status(500)
            self.finish()

    def memory_check(self):
        '''Method to check memory on the node'''
        return True

class TestTracker(TrackerDataHandler):
    '''Handler for test requests.'''

    def initialize(self, version):
        '''Initialize the logger.'''
        super(TestTracker, self).initialize()
        self.version = version
    
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self, *args, **kwargs):
        '''Handle a test tracking request.'''
        try:
            tracker_data = yield self.parse_tracker_data(self.version)
        except tornado.web.HTTPError as e:
            raise
        except NotInterestingData as e:
            pass
        except Exception as err:
            _log.exception("key=test_track msg=%s", err) 
            self.set_status(500)
            self.finish()
            return
        
        self.set_status(200)
        self.add_header("content-type", "application/javascript") 
        self.finish()

class ErrorMessageTracker(TrackerDataHandler):
    ''' Log error message from the tracker '''

    def initialize(self, version):
        '''Initialize the logger.'''
        super(ErrorMessageTracker, self).initialize()
        self.version = version
        self.tag = 'jstracker'

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self, *args, **kwargs):
        '''Handle a test tracking request.'''
        try:
            # v1 schema, change as appropriate
            msg = {"error_msg": self.request.uri.split('?error=')[-1]}
            log_data = ("PLAINTEXT=" + 
                    urllib2.quote(json.dumps(msg)))
            loggly_request = tornado.httpclient.HTTPRequest(
                    '%s/tag/%s' % (options.loggly_base_url, self.tag), method='POST', 
                    headers={'Content-type' : 'application/x-www-form-urlencoded',
                        'Content-length' : len(log_data) },
                    body=log_data)
            # best effort
            resp = yield tornado.gen.Task(utils.http.send_request, loggly_request)
        except tornado.web.HTTPError as e:
            raise
        except NotInterestingData as e:
            pass
        except Exception as err:
            _log.exception("key=test_track msg=%s", err) 
            self.set_status(500)
            self.finish()
            return
        
        self.set_status(200)
        self.add_header("content-type", "application/javascript") 
        self.finish()

###########################################
# File Backup Handler thread 
###########################################
class FileBackupHandler(threading.Thread):
    '''Thread that uploads data to S3.'''
    
    def __init__(self, dataQ, watcher=utils.ps.ActivityWatcher()):
        super(FileBackupHandler, self).__init__()
        self.dataQ = dataQ
        self.daemon = True
        self.watcher = watcher
        self.backup_stream = None
        self.protocol_writer = None
        self.events_in_file = 0

        statemon.state.qsize = self.dataQ.qsize()

        # Make sure the backup directory exists
        if not os.path.exists(options.backup_disk):
            os.makedirs(options.backup_disk)

    def __del__(self):
        if self.backup_stream is not None:
            self.backup_stream.close()

    def _generate_log_filename(self):
        '''Create a new log filename.'''
        return '%s_%s_clicklog.log' % (
            time.strftime('%S%M%H%d%m%Y', time.gmtime()),
            shortuuid.uuid())

    def _open_new_backup_file(self):
        '''Opens a new backup file and puts it on self.backup_stream.'''
        if not os.path.exists(options.backup_disk):
            os.makedirs(options.backup_disk)
            
        backup_file = \
          open(os.path.join(options.backup_disk,
                            self._generate_log_filename()),
                            'wb')
        self.backup_stream = TTransport.TFileObjectTransport(
            backup_file)
        self.protocol_writer = TCompactProtocol.TCompactProtocol(
            self.backup_stream)

    def _prepare_backup_stream(self):
        '''Prepares the backup stream for writing to.

        This could mean flushing it to disk or closing this file and
        opening a new one.
        '''
        if self.backup_stream is None:
            self._open_new_backup_file()

        # See if the file should be flushed
        if self.events_in_file % options.flume_flush_interval == 0:
            self.backup_stream.flush()

        # Check to see if the file should be rolled over
        if self.events_in_file >= options.backup_max_events_per_file:
            self.backup_stream.close()
            self._open_new_backup_file()
            self.events_in_file = 0

    def run(self):
        '''Main runner for the handler.'''
        while True:
            try:
                try:
                    event = self.dataQ.get(True, 30)
                except Queue.Empty:
                    if self.backup_stream is not None:
                        self.backup_stream.flush()
                    continue

                with self.watcher.activate():
                    statemon.state.qsize = self.dataQ.qsize()
                    self._prepare_backup_stream()

                    event.write(self.protocol_writer)
                    self.events_in_file += 1
            except Exception as err:
                _log.exception("key=file_backup_handler msg=%s", err)

            self.dataQ.task_done()

class HealthCheckHandler(TrackerDataHandler):
    '''Handler for health check ''' 

    def initialize(self, flume_port):
        super(HealthCheckHandler, self).initialize()
        self.flume_port = flume_port
    
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        '''Handle a test tracking request.'''

        # see if we can connect to the flume port
        try:
            sock = socket.create_connection(('localhost', self.flume_port),
                                            3)
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()

            # see if we can connect to the image serving platform
            try:
                sock = socket.create_connection(('localhost',
                                                 self.isp_port),
                                                 3)
                sock.shutdown(socket.SHUT_RDWR)
                sock.close()
                self.write("<html> Server OK </html>")
                self.set_status(200)
            except socket.error:
                _log.error('Could not open isp port')
                self.set_status(500)
                
        except socket.error:
            _log.error('Could not open flume port')
            self.set_status(500)

        self.finish()

###########################################
# Create Tornado server application
###########################################

class Server(threading.Thread):
    '''The server, which can be run as it's own thread.

    Or just call run() directly to have it startup and block.
    '''
    def __init__(self, watcher=utils.ps.ActivityWatcher()):
        '''Create the server. 

        Inputs:
        
        watcher - Optional synchronization object that can be used to
        know when the server is active.
        
        '''
        super(Server, self).__init__()
        self.backup_queue = Queue.Queue()
        self.backup_handler = FileBackupHandler(self.backup_queue, watcher)
        self.io_loop = tornado.ioloop.IOLoop()
        self._is_running = threading.Event()
        self._watcher = watcher

        # Figure out the message schema
        with open(options.message_schema) as f:
            schema_str = f.read()
        schema = avro.schema.parse(schema_str)
        schema_hash = hashlib.md5(schema_str).hexdigest()
        schema_url = ('http://%s.s3.amazonaws.com/%s.avsc' % 
                      (options.schema_bucket, schema_hash))
        avro_writer = avro.io.DatumWriter(schema)
        self.flume_buffer = FlumeBuffer(options.flume_port, self.backup_queue)

        # Make sure that the schema exists at a URL that can be reached
        response = utils.http.send_request(
            tornado.httpclient.HTTPRequest(schema_url), 2)
        if response.error:
            _log.fatal('Could not find schema at %s. '
                       'Did you run schema/compile_schema.py?' % 
                       schema_url)
            raise response.error

        self.application = tornado.web.Application([
            (r"/v2", LogLines, dict(watcher=self._watcher,
                                    version=2,
                                    avro_writer=avro_writer,
                                    schema_url=schema_url,
                                    flume_buffer=self.flume_buffer)),
            (r"/v2/track", LogLines, dict(watcher=self._watcher,
                                          version=2,
                                          avro_writer=avro_writer,
                                          schema_url=schema_url,
                                          flume_buffer=self.flume_buffer
                                          )),
            (r"/v2/test", TestTracker, dict(version=2)),
            (r"/v2/error", ErrorMessageTracker, dict(version=2)),
            (r"/healthcheck", HealthCheckHandler,
             dict(flume_port=options.flume_port)),
            ])

    def run(self):
        statemon.state.flume_errors = 0
        statemon.state.messages_handled = 0
        statemon.state.invalid_messages = 0
        
        with self._watcher.activate():
            self.backup_handler.start()
            self.io_loop.make_current()
            
            server = tornado.httpserver.HTTPServer(self.application,
                                                   io_loop=self.io_loop,
                                                   xheaders=True)
            utils.ps.register_tornado_shutdown(server)
            server.listen(options.port)
        

            self._is_running.set()
        self.io_loop.start()
        server.stop()

        # Flush any extra events in the buffer
        self.flume_buffer.flush()

    @tornado.gen.engine
    def wait_until_running(self):
        '''Blocks until the server/io_loop is running.'''
        self._is_running.wait()
        yield tornado.gen.Task(self.io_loop.add_callback)

    def wait_for_processing(self):
        '''Blocks until the current requests are all processed.'''
        self.event_queue.join()

    def stop(self):
        '''Stops the server'''
        self.io_loop.stop()

def main(watcher=utils.ps.ActivityWatcher()):
    '''Main function that runs the server.'''
    with watcher.activate():
        server = Server(watcher)
    server.run()
    

# ============= MAIN ======================== #
if __name__ == "__main__":
    utils.neon.InitNeon()
    #Turn off Access logs for tornado
    logging.getLogger('tornado.access').propagate = False
    main()
