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
import tornado.gen
from utils import http
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
        added_jobs = 0
        videos = search_results['docs'] 
        last_processed_date = None 
        _log.info('Processing %d videos for cnn' % (len(videos))) 
        for video in videos:
            try:
                video_id = video['videoId'].replace('/', '~') 
                publish_date = last_processed_date = video['firstPublishDate']
                title = video.get('title', 'no title')
                duration = video.get('duration', None)
                thumb, thumb_id = self._get_best_image_info(video['relatedMedia'])
                custom_data = self._build_custom_data_from_topics(video['topics'])
                video_src = self._find_best_cdn_url(video['cdnUrls'])
                existing_video = yield tornado.gen.Task(neondata.VideoMetadata.get, 
                                                        neondata.InternalVideoID.generate(self.account_id, video_id))
                if not existing_video:
                    response = yield self.submit_video(video_id=video_id, 
                                                       video_url=video_src, 
                                                       external_thumbnail_id=thumb_id, 
                                                       custom_data=custom_data, 
                                                       duration=duration, 
                                                       publish_date=publish_date, 
                                                       video_title=unicode(title), 
                                                       default_thumbnail=thumb)
                    if response['job_id']:
                        added_jobs += 1
            except KeyError as e:
                # let's continue here, we do not have enough to submit 
                pass 
            except Exception as e: 
                statemon.state.increment('unexpected_submission_error')
                _log.exception('Unknown error occured on video_id %s exception = %s' % (video_id, e))
                pass
         
        if last_processed_date:
            def _modify_me(x): 
                x.last_process_date = last_processed_date 
            yield tornado.gen.Task(neondata.CNNIntegration.modify, self.platform.integration_id, _modify_me)

        _log.info('Added %d jobs for cnn integration' % added_jobs) 
        raise tornado.gen.Return(self.platform)
                
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
            match = re.match(key) 
            if match:  
                groups = match.groups() 
                cur_reso = groups[0] * groups[1] 
                if cur_reso > max_reso 
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
