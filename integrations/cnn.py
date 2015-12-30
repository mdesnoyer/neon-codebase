'''
Functions for the CNN integration

Authors: David Lea (lea@neon-lab.com)
Copyright 2015 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.cnn_api
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

class CNNIntegration(integrations.ovp.OVPIntegration):
    def __init__(self, account_id, integration):
        super(CNNIntegration, self).__init__(account_id, integration)
        # this is the user.neon_api_key, but properly named here
        self.account_id = account_id 
        self.api = api.cnn_api.CNNApi(integration.api_key_ref)
        self.last_process_date = integration.last_process_date
        # to correspond with the inherited class 
        self.platform = integration
        self.platform.neon_api_key = account_id

 
    @tornado.gen.coroutine 
    def submit_new_videos(self):
        search_results = yield self.api.search(dateutil.parser.parse(self.last_process_date))
        videos = search_results['docs'] 
        _log.info('Processing %d videos for cnn' % (len(videos)))
        self.set_video_iter(videos) 
        yield self.submit_ovp_videos(self.get_next_video_item, grab_new_thumb=False)
 
        #_log.info('Added %d jobs for cnn integration' % added_jobs) 
        raise tornado.gen.Return(self.platform)

    def get_video_id(self, video):
        '''override from ovp''' 
        return video['videoId'].replace('/', '~')
 
    def get_video_url(self, video):
        '''override from ovp''' 
        return self._find_best_cdn_url(video['cdnUrls'])

    def get_video_callback_url(self, video):
        '''override from ovp''' 
        return None 

    def get_video_title(self, video):
        '''override from ovp''' 
        return video.get('title', 'no title')

    def get_video_custom_data(self, video):
        '''override from ovp''' 
        return self._build_custom_data_from_topics(video['topics'])

    def get_video_duration(self, video):
        '''override from ovp''' 
        duration = video.get('duration', None)
        if duration:
            ts = time.strptime(duration, "%H:%M:%S")
            duration = datetime.timedelta(hours=ts.tm_hour, 
                                          minutes=ts.tm_min, 
                                          seconds=ts.tm_sec).total_seconds()
        return duration

    def get_video_publish_date(self, video):
        '''override from ovp'''
        return video['firstPublishDate'] 

    def get_video_thumbnail_info(self, video):
        '''override from ovp''' 
        thumb_url, thumb_ref = self._get_best_image_info(video['relatedMedia'])
        return { 'thumb_url' : thumb_url, 
                 'thumb_ref' : thumb_ref }

    def set_video_iter(self, videos):
        self.video_iter = iter(videos) 
    
    @tornado.gen.coroutine 
    def get_next_video_item(self):
        video = None 
        try:  
            video = self.video_iter.next()
        except StopIteration: 
            video = StopIteration('hacky')
  
        raise tornado.gen.Return(video)  
                
    @staticmethod
    def _get_best_image_info(media_json):
        '''Returns the (url, {image_struct}) of the best image in the 
           CNN related media object
        '''
        if media_json.has_key('media'):
            #now we need to iter on the items as there could be multiple images here
            for item in media_json['media']:
                try: 
                    if item['type'] == 'image':
                        cuts = item['cuts']
                        thumb_id = item['imageId']
                        if cuts.has_key('exlarge16to9'):
                            return cuts['exlarge16to9']['url'], thumb_id
                except KeyError as e: 
                    continue 
        else:
            return None
    
    @staticmethod 
    def _find_best_cdn_url(cdn_urls):
        max_reso = 0 
        current_url = None 
        p = re.compile('(\d*)x(\d*)_\d*k_mp4')
        for key in cdn_urls.keys():
            match = p.match(key) 
            if match:  
                groups = match.groups() 
                cur_reso = int(groups[0]) * int(groups[1]) 
                if cur_reso > max_reso: 
                    max_reso = cur_reso 
                    current_url = cdn_urls[key]
 
        if current_url: 
            return current_url 
        else: 
            raise Exception('unable to find a suitable url for processing') 
 
    @staticmethod 
    def _build_custom_data_from_topics(topics):
        custom_data = {} 
        custom_data['topics'] = [] 
        for t in topics: 
           custom_data['topics'].append(t)  
        return custom_data
    
    @tornado.gen.coroutine
    def process_publisher_stream(self):
        yield self.submit_new_videos()
