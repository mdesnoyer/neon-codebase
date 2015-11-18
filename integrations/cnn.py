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
        search_results = yield self.api.search(self.last_process_date)
        added_jobs = 0
        videos = json.loads(search_results)['docs'] 
        last_processed_date = None 
        _log.info('Processing %d videos for cnn' % (len(videos))) 
        for video in videos:
            try:
                video_id = video.get('videoId').replace('/', '-') 
                publish_date = last_processed_date = video.get('firstPublishDate')
                title = video.get('title')
                duration = video.get('duration')
                thumb, thumb_id = self._get_best_image_info(video['relatedMedia'])
                custom_data = self._build_custom_data_from_topics(video['topics']) 
                video_src = video['cdnUrls']['1920x1080_5500k_mp4'] 
                existing_video = yield tornado.gen.Task(neondata.VideoMetadata.get, 
                                                        neondata.InternalVideoID.generate(self.account_id, video_id))
                if not existing_video:
                    response = yield self.submit_video(video_id, 
                                                       video_src, 
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
                continue 
            except Exception as e: 
                statemon.state.increment('unexpected_submission_error')
                _log.exception('Unknown error occured on video_id %s exception = %s' % (video_id, e))
                pass
         
        if last_processed_date:
            self.internal_integration.last_process_date = last_processed_date
            yield self.internal_integration.save()
 
        _log.info('Added %d jobs for cnn integration' % added_jobs) 
        raise tornado.gen.Return(True)
                
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
    def _build_custom_data_from_topics(topics):
        custom_data = {} 
        custom_data['topics'] = [] 
        for t in topics: 
           custom_data['topics'].append(t)  
        return custom_data
    
    @tornado.gen.coroutine
    def process_publisher_stream(self):
        yield self.submit_new_videos()
