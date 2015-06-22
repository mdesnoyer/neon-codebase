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
    def _get_submit_video_fields():
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
            video_urls[f_width] = url 
       
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
        retval = {}
            
        try:
            bc_video_info = yield self.bc_api.find_videos_by_ids(
                ovp_video_ids,
                BrightcoveIntegration._get_submit_video_fields(),
                async=True)
        except brightcove_api.BrightcoveApiServerError as e:
            statemon.state.increment('bc_api_errors')
            _log.error('Error getting data from Brightcove: %s' % e)
            raise integrations.ovp.OVPError(e)

        for bc_video_id, data in bc_video_info.iteritems():
            thumb_url, thumb_data = \
              BrightcoveIntegration._get_best_image_info(data)
            if thumb_url is None or data['length'] < 0:
                _log.warn('Brightcove id %s is a live stream' % bc_video_id)
                continue
            
            try:
                response = yield self.submit_video(
                    bc_video_id,
                    self._get_video_url_to_download(data),
                    video_title=data['name'],
                    default_thumbnail=thumb_url,
                    external_thumbnail_id=unicode(thumb_data['id']))
                retval[bc_video_id] = response['job_id']

            except integrations.ovp.CMSAPIError as e:
                retval[bc_video_id] = e
                continue

            # TODO: Move this to the video server for when the
            # video is processed.
            res = yield tornado.gen.Task(
                neondata.BrightcovePlatform.modify,
                self.platform.neon_api_key,
                self.platform.integration_id,
                lambda x: x.add_video(bc_video_id, response['job_id']))

        raise tornado.gen.Return(retval)
