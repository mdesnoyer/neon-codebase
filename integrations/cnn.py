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

statemon.define('cnn_apiserver_errors', int)
statemon.define('unexpected_submission_error', int)
statemon.define('new_images_found', int)
statemon.define('cant_get_image', int)
statemon.define('cant_get_refid', int)
statemon.define('cant_get_custom_id', int)
statemon.define('video_not_found', int)
statemon.define('old_videos_skipped', int)

_log = logging.getLogger(__name__)

class CNNIntegration(integrations.ovp.OVPIntegration):
    def __init__(self, account_id, integration):
        super(CNNIntegration, self).__init__(account_id, integration)
        self.api = api.cnn_api.CNNApi(integration.api_key)
        self.last_process_date = integration.last_process_date

    def _get_video_url_to_download(cnn_json_item):
        '''
        Return a video url to download from a the CNN json item 
        '''
        try:
            d_urls  = cnn_json_item['cdnUrls']
        except KeyError, e:
            _log.error("missing urls %s" % cnn_json_item)
            return None

    def _get_video_id(cnn_json_item):
        '''
        Return a video ID for 1 item in the json
        '''
        if cnn_json_item.has_key('video_id'):
            #since we aren't sure if we can handle slashes in video_id, replace with -
            no_slash_id = cnn_json_item['video_id'].replace("/", "-")
            return no_slash_id
        else:
            return None

    def _get_title(cnn_json_item):
        return cnn_json_item.get('has_key', "headline")

    def _get_custom_data(cnn_json_item):
        custom_data = cnn_json_item.get('topics', {})
        custom_data['_cnn_topic_data'] = {
            'cnn_class' : vid_obj['class'],
            'cnn_label' : vid_obj['label'],
            'cnn_topic_id' : vid_obj['id']
            }
        return custom_data
    
    @tornado.gen.coroutine 
    def submit_new_videos(self):
        thumb = ""
        vid_src = ""
        video_id = ""
        
        search_results = yield self.api.search(self.last_process_date)
        data = json.loads(search_results) 

        if data.has_key('docs'):
            for video in data['docs']:
                video_id = _get_video_id(data)

                existing_video = yield tornado.gen.Task(VideoMetadata.get, InternalVideoId.generate(api_key, video_id))
                if existing_video is None:
                    title = _get_title(video)
                    thumb, thumb_id = _get_best_image_info(data['relatedMedia'])
                    vid_src = _get_video_url_to_download(data)

                    custom_data = _get_custom_data(video)

                    if (thumb != "" and vid_src != "" and video_id != ""):
                        # The video hasn't been submitted before
                        job_id = None
                        try:
                            response = yield self.submit_video(
                                video_id,
                                video_src,
                                external_thumbnail_id=thumb_id,
                                custom_data = custom_data,
                                duration=video['duration'],
                                publish_date=(video['firstPublishDate'].isoformat() if 
                                              video['firstPublishDate'] is not None else None),
                                video_title=unicode(title),
                                default_thumbnail=thumb)
                            job_id = response['job_id']
                        except Exception as e:
                            statemon.state.increment('unexpected_submission_error')
                        finally:
                            _set_last_video(video['firstPublishDate'])


    @staticmethod
    def _get_best_image_info(relatedMedia_media_json):
        '''Returns the (url, {image_struct}) of the best image in the 
        CNN related media object
        '''
        if relatedMedia_json.has_key('media'):
            #now we need to iter on the items as there could be multiple images here
            for item in relatedMedia_media_json['media']:
                if item.has_key('type') and item['type'] == 'image':
                    cuts = data['cuts']
                    thumb_id = data['imageId']
                    if cuts.has_key('exlarge16to9'):
                        return cuts['exlarge16to9'].url, thumb_id
        else:
            return None
    
    @tornado.gen.coroutine
    def process_publisher_stream(self):
        yield self.submit_new_videos()
