'''
Functions for the Fox integration

Copyright 2015 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.fox_api
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

class FoxIntegration(integrations.ovp.OVPIntegration):
    def __init__(self, account_id, cmsdb_integration):
        super(FoxIntegration, self).__init__(account_id, cmsdb_integration)
        
        self.account_lookup_id = account_id 
        self.api = api.fox_api.FoxApi(cmsdb_integration.feed_pid_ref)
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

        search_results = yield self.api.search(from_date)
        videos = search_results['entries'] 
        self.set_video_iter(videos) 
        _log.info('Processing %d videos for fox integration' % len(videos)) 
        yield self.submit_ovp_videos(self.get_next_video_item, grab_new_thumb=False)
 
        raise tornado.gen.Return(self.platform)

    def get_video_id(self, video):
        '''override from ovp''' 
        return video['id'].replace('http://data.media.theplatform.com/media/data/Media/', '')
 
    def get_video_url(self, video):
        '''override from ovp'''
        return video['media$content'][0]['plfile$streamingUrl']  

    def get_video_callback_url(self, video):
        '''override from ovp''' 
        return None 

    @staticmethod
    def get_video_title(video):
        '''override from ovp''' 
        return video.get('title', u'no title')

    def get_video_custom_data(self, video):
        '''override from ovp''' 
        custom_data = {}

        try:  
            custom_data['keywords'] = video['media$keywords'] 
        except KeyError: 
            pass 
        try: 
            custom_data['showcode'] = video['fox$showcode'] 
        except KeyError: 
            pass 
        try: 
            custom_data['content_type'] = video['fox$contentType']         
        except KeyError: 
            pass 

        return custom_data

    def get_video_duration(self, video):
        '''override from ovp'''
        return video['media$content'][0]['plfile$duration'] 

    def get_video_publish_date(self, video):
        '''override from ovp'''
        return video['pubDate'] / 1000. 

    def get_video_thumbnail_info(self, video):
        '''override from ovp'''
        url = video['plmedia$defaultThumbnailUrl']
        thumb_url = url 
        thumb_ref = url.replace('/', '~').replace('_', '~')
        return { 'thumb_url' : thumb_url, 
                 'thumb_ref' : thumb_ref }

    def set_video_iter(self, videos):
        self.video_iter = iter(videos)
 
    def does_video_exist(self, video_meta, video_ref): 
        return video_meta is not None   
 
    def get_video_last_modified_date(self, video):
        return int(video['updated']) / 1000.0 
    
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

    def _log_statemon_submit_video_error(self):
        statemon.state.define_and_increment(
            'submit_video_fox_error.%s' % self.account_id)
