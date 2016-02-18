'''
Functions for the microsoft integration

Copyright 2015 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.microsoft_api
import calendar
from cmsdb import neondata
import datetime
import dateutil.parser
import integrations.ovp
import json
import logging
import re
import time
import tornado.gen
from utils import http
from utils.inputsanitizer import InputSanitizer
from utils.options import options, define
from utils import statemon

statemon.define('unexpected_submission_error', int)

_log = logging.getLogger(__name__)

class MicrosoftIntegration(integrations.ovp.OVPIntegration):
    def __init__(self, account_id, cmsdb_integration):
        super(MicrosoftIntegration, self).__init__(account_id, cmsdb_integration)
        
        self.account_lookup_id = account_id 
        self.api = api.microsoft_api.MicrosoftApi(cmsdb_integration.feed_id)
        self.last_process_date = cmsdb_integration.last_process_date
        # to correspond with the inherited class 
        self.platform = cmsdb_integration

    @tornado.gen.coroutine 
    def submit_new_videos(self):
        acct = yield tornado.gen.Task(neondata.NeonUserAccount.get,
                                      self.account_lookup_id)
        self.neon_api_key = acct.neon_api_key
        self.account_id = acct.account_id
        from_date = datetime.datetime(1970, 1, 1)
        if self.platform.last_process_date is not None: 
            from_date = datetime.datetime.utcfromtimestamp(self.last_process_date)

        videos = yield self.api.search(from_date)
        self.set_video_iter(videos) 
        _log.info('Processing %d videos for microsoft integration' % len(videos)) 
        yield self.submit_ovp_videos(self.get_next_video_item, grab_new_thumb=False)
 
        raise tornado.gen.Return(self.platform)

    def get_video_id(self, video):
        '''override from ovp''' 
        return video['_id']
 
    def get_video_url(self, video):
        '''override from ovp'''
        videoFiles = video['videoFiles'] 
        url = None 
        for vf in videoFiles: 
            if vf['format'] is '1001': 
                url = vf['sourceHref'] 
                break
 
        return url  

    def get_video_callback_url(self, video):
        '''override from ovp''' 
        return None 

    def get_video_title(self, video):
        '''override from ovp''' 
        return video.get('title', 'no title')

    def get_video_custom_data(self, video):
        '''override from ovp''' 
        custom_data = {}

        try:  
            custom_data['labels'] = video['labels'] 
        except KeyError: 
            pass 
        try: 
            custom_data['contentQuality'] = video['contentQuality'] 
        except KeyError: 
            pass 
        try: 
            custom_data['facets'] = video['facets']         
        except KeyError: 
            pass 
        try: 
            custom_data['abstract'] = video['abstract']         
        except KeyError: 
            pass 

        return custom_data

    def get_video_duration(self, video):
        '''override from ovp'''
        return video['playTime'] 

    def get_video_publish_date(self, video):
        '''override from ovp'''
        dt = dateutil.parser.parse(video['_lastPublishedDateTime'])
        ts = calendar.timegm(dt.timetuple())
        return ts

    def get_video_thumbnail_info(self, video):
        '''override from ovp'''
        thumb_url = video['thumbnail']['address']
        thumb_ref = video['thumbnail']['id'] 
        return { 'thumb_url' : thumb_url, 
                 'thumb_ref' : thumb_ref }

    def set_video_iter(self, videos):
        self.video_iter = iter(videos)
 
    def does_video_exist(self, video_meta, video_ref): 
        return video_meta is not None   
 
    def get_video_last_modified_date(self, video):
        dt = dateutil.parser.parse(video['_lastEditedDateTime'])
        ts = calendar.timegm(dt.timetuple())
        return ts
    
    @tornado.gen.coroutine 
    def get_next_video_item(self):
        video = None 
        try:  
            video = self.video_iter.next()
        except StopIteration: 
            video = StopIteration('hacky')
  
        raise tornado.gen.Return(video)  
                
    @tornado.gen.coroutine
    def process_publisher_stream(self):
        yield self.submit_new_videos()
