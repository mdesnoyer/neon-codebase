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

def _normalize_thumbnail_url(url):
    '''Returns a thumb url without transport mechanism or query string.'''
    if url is None:
        return None
    parse = urlparse.urlparse(url)
    if re.compile('(brightcove)|(bcsecure)').search(parse.netloc):
        # Brightcove can move the image around, but its basename will
        # remain the same, so if it is a brightcove url, only look at
        # the basename.
        return 'brightcove.com/%s' % (os.path.basename(parse.path))
    return '%s%s' % (parse.netloc, parse.path)

def _get_urls_from_bc_response(response):
    urls = []
    for image_type in ['thumbnail', 'videoStill']:
        urls.append(response.get(image_type + 'URL', None))
        if response.get(image_type, None) is not None:
            urls.append(response[image_type].get('remoteUrl', None))

    return [x for x in urls if x is not None]

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
            if video_urls.has_key(self.platform.rendition_frame_width):
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

        for data in bc_video_info:
            bc_video_id = data['id']
            try:
                retval[bc_video_id] = self.submit_one_video_object(bc_video_id,
                                                                   data)
            except integrations.ovp.CMSAPIError as e:
                retval[bc_video_id] = e

        raise tornado.gen.Return(retval)

    @tornado.gen.coroutine
    def submit_one_video_object(self, vid_obj, grab_new_thumb=True):
        '''Submits one video object

        Inputs:
        vid_obj - A video object from the response from the Brightcove API
        grab_new_thumb - True if new thumbnails for the video should be grabbed

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
            if grab_new_thumb:
                yield self._grab_new_thumb(vid_obj)

        raise tornado.gen.Return(job_id)

    @tornado.gen.coroutine
    def _grab_new_thumb(self, data):
        '''Grab a new thumbnail from a video object if there is one.

        Inputs:
        data - A video object from Brightcove
        '''
        bc_video_id = data['id']

        thumb_url, thumb_data = \
          BrightcoveIntegration._get_best_image_info(vid_obj)
        bc_urls = [_normalize_url(x) for x in _get_urls_from_bc_response(data)]

        # Function that will set the external id in the ThumbnailMetadata
        external_id = thumb_data.get('id', None)
        def _set_external_id(obj):
            obj.external_id = external_id

        # Get the video object from our database
        video_id = neondata.InternalVideoID.generate(platform.neon_api_key,
                                                     bc_video_id)
        vid_meta = yield tornado.gen.Task(neondata.VideoMetadata.get,
                                          video_id)
        if not vid_meta:
            _log.warn('Could not find video %s' % video_id)
            statemon.state.increment('video_not_found')
            return

        # Search for the thumbnail already in our database
        thumbs = yield tornado.gen.Task(neondata.ThumbnailMetadata.get_many,
                                        vid_meta.thumbnail_ids)
        found_thumb = False
        min_rank = 1
        for thumb in thumbs:
            if (thumb is None or 
                thumb.type != neondata.ThumbnailType.BRIGHTCOVE):
                continue

            if thumb.rank < min_rank:
                min_rank = thumb.rank

            if thumb.external_id is not None:
                # We know about this thumb was in Brightcove so see if it
                # is still there.
                if thumb.external_id == external_id:
                    found_thumb = True
            elif thumb.refid is not None:
                # For legacy thumbs, we specified a reference id. Look for it
                if thumb.refid == thumb_data.get('referenceId', None):
                    found_thumb = True

                    yield tornado.gen.Task(neondata.ThumbnailMetadata.modify,
                                           thumb.key,
                                           _set_external_id)
            else:
                # We do not have the id for this thumb, so see if we
                # can match the url.
                norm_urls = set([normalize_url(x) for x in thumb.urls])
                if len(norm_urls.intersection(bc_urls)) > 0:
                    found_thumb = True
                    
                    # Now update the external id because we didn't
                    # know about it before.
                    yield tornado.gen.Task(neondata.ThumbnailMetadata.modify,
                                           thumb.key,
                                           _set_external_id)

        if not found_thumb:
            # Add the thumbnail to our system
            urls = _get_urls_from_bc_response(data)
            added_image = False
            for url in urls[::-1]:
                try:
                    new_thumb = neondata.ThumbnailMetadata(
                        None,
                        ttype=neondata.ThumbnailType.BRIGHTCOVE,
                        rank = min_rank-1,
                        external_id = external_id
                        )
                    yield vid_meta.download_and_add_thumbnail(
                        new_thumb,
                        url,
                        save_objects=True,
                        async=True)
            
                    _log.info(
                        'Found new thumbnail %s for video %s at Brigthcove.' %
                        (external_id, vid_meta.key))
                    statemon.state.increment('new_images_found')
                    added_image = True
                    break
                except IOError:
                    # Error getting the image, so keep going
                    pass
            if not added_image:
                _log.error('Could not find valid image to add to video %s. '
                           'Tried urls %s' % (vid_meta.key, urls))
                statemon.state.increment('cant_get_image')
        
