'''
Functions for the Brightcove integration

Authors: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2015 Neon Labs
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from api import brightcove_api
from cmsdb import neondata
import integrations.ovp
import logging
import tornado.gen
from utils import statemon

statemon.define('bc_api_errors', int)

_log = logging.getLogger(__name__)

class BrightcoveIntegration(integrations.ovp.OVPIntegration):
    def __init__(self, account_id, platform):
        super(BrightcoveIntegration, self).__init__(account_id, platform)
        self.bc_api = brightcove_api.BrightcoveApi(
            self.platform.neon_api_key, self.platform.publisher_id,
            self.platform.read_token, self.platform.write_token)

    @staticmethod
    def get_submit_video_fields():
        '''Return a list of brightcove video fields needed to be able to
        submit jobs to our CMSAPI.
        '''
        return ['id',
                'videoStill',
                'videoStillURL',
                'thumbnail',
                'thumbnailURL',
                'FLVURL',
                'renditions',
                'length',
                'name',
                'publishedDate']

    def _get_video_url_to_download(self, b_json_item):
        '''
        Return a video url to download from a brightcove json item 
        
        if frame_width is specified, get the closest one  
        '''

        video_urls = {}
        try:
            d_url  = b_json_item['FLVURL']
        except KeyError, e:
            _log.error("missing flvurl")
            return

        #If we get a broken response from brightcove api
        if not b_json_item.has_key('renditions'):
            return d_url

        renditions = b_json_item['renditions']
        for rend in renditions:
            f_width = rend["frameWidth"]
            url = rend["url"]
            if url is not None:
                video_urls[f_width] = url
            elif rend['remoteUrl'] is not None:
                video_urls[f_width] = rend['remoteUrl']
       
        # no renditions
        if len(video_urls.keys()) < 1:
            return d_url
        
        if self.platform.rendition_frame_width:
            if video_urls.has_key(fself.platform.rendition_frame_width):
                return video_urls[self.platform.rendition_frame_width] 
            closest_f_width = min(
                video_urls.keys(),
                key=lambda x:abs(x-self.platform.rendition_frame_width))
            return video_urls[closest_f_width]
        else:
            #return the max width rendition
            return video_urls[max(video_urls.keys())]

    @staticmethod
    def _get_best_image_info(data):
        '''Returns the (url, {image_struct}) of the best image in the 
        Brightcove video object
        '''
        url = data['videoStillURL']
        if url is None:
            url = data['thumbnailURL']
            if url is None:
                return (None, None)
            obj = data['thumbnail']
        else:
            obj = data['videoStill']

        return url, obj
        

    @tornado.gen.coroutine
    def lookup_and_submit_videos(self, ovp_video_ids):
        '''Looks up a list of video ids and submits them.'''
        retval = {}
            
        try:
            bc_video_info = yield self.bc_api.find_videos_by_ids(
                ovp_video_ids,
                BrightcoveIntegration.get_submit_video_fields(),
                async=True)
        except brightcove_api.BrightcoveApiServerError as e:
            statemon.state.increment('bc_api_errors')
            _log.error('Error getting data from Brightcove: %s' % e)
            raise integrations.ovp.OVPError(e)

        for bc_video_id, data in bc_video_info.iteritems():
            try:
                retval[bc_video_id] = self.submit_one_video_object(bc_video_id,
                                                                   data)
            except integrations.ovp.CMSAPIError as e:
                retval[bc_video_id] = e

        raise tornado.gen.Return(retval)

    @tornado.gen.coroutine
    def submit_one_video_object(self, vid_obj):
        '''Submits one video object

        Inputs:
        vid_obj - A video object from the response from the Brightcove API

        Outputs:
        the job id
        '''
        thumb_url, thumb_data = \
              BrightcoveIntegration._get_best_image_info(vid_obj)
        if (thumb_url is None or 
            vid_obj['length'] < 0 or 
            thumb_url.endswith('.m3u8') or 
            thumb_url.startswith('rtmp://') or 
            thumb_url.endswith('.csmil')):
            _log.warn('Brightcove id %s is a live stream' % vid_obj['id'])
            raise tornado.gen.Return(None)
            

        if not vid_obj['id'] in self.platform.videos:
            # The video hasn't been submitted before
            response = yield self.submit_video(
                vid_obj['id'],
                self._get_video_url_to_download(vid_obj),
                video_title=vid_obj['name'],
                default_thumbnail=thumb_url,
                external_thumbnail_id=unicode(thumb_data['id']))

            # TODO: Move this to the video server for when the
            # video is processed.
            self.platform = yield tornado.gen.Task(
                neondata.BrightcovePlatform.modify,
                self.platform.neon_api_key,
                self.platform.integration_id,
            lambda x: x.add_video(vid_obj['id'], response['job_id']))

            job_id = response['job_id']
        else:
            job_id = self.platform.videos[vid_obj['id']]

        raise tornado.gen.Return(job_id)
        
