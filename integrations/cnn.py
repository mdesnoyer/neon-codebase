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

from cmsdb import neondata
import datetime
import dateutil.parser
import integrations.ovp
import logging
import re
import tornado.gen
import urllib2
import urlparse
from utils.options import options, define
from utils import statemon


statemon.define('cnn_apiserver_errors', int)
statemon.define('cnn_apiclient_errors', int)
statemon.define('unexpected_submition_error', int)
statemon.define('new_images_found', int)
statemon.define('cant_get_image', int)
statemon.define('cant_get_refid', int)
statemon.define('cant_get_custom_id', int)
statemon.define('video_not_found', int)
statemon.define('old_videos_skipped', int)

_log = logging.getLogger(__name__)


class CNNIntegration(integrations.ovp.OVPIntegration):
    def __init__(self, account_id, platform):
        super(CNNIntegration, self).__init__(account_id, platform)

    @staticmethod
    def get_submit_video_fields():
        '''Return a list of CNN feed fields needed to be able to
        submit jobs to our CMSAPI.
        '''
        return ['sourceId',
                'exlarge16to9',
                '1920x1080_5500k_mp4']

    def _get_video_url_to_download(cnn_json_item):
        '''
        Return a video url to download from a the CNN json item 
        '''

        try:
            d_urls  = cnn_json_item['cdnUrls']
        except KeyError, e:
            _log.error("missing urls %s" % cnn_json_item)
            return None

        #If we get a broken response from brightcove api
        if d_urls.has_key('1920x1080_5500k_mp4'):
            return d_urls['1920x1080_5500k_mp4']
        else:
            return None

    def _make_CNN_feed_request(request_url):
        req = urllib2.Request(request_url, headers=headers)
        res = urllib2.urlopen(req)
            return json.loads(res.read())   

    def _get_videoID(cnn_json_item):
         '''
        Return a video ID for 1 item in the json
        '''
        if cnn_json_item.has_key('sourceId'):
            return cnn_json_item['sourceId']
        else
            return None


    @staticmethod
    def _get_best_image_info(relatedMedia_media_json):
        '''Returns the (url, {image_struct}) of the best image in the 
        CNN related media  object
        '''
        if relatedMedia_media_json.has_key('cuts'):
            cuts = data['cuts']
            if cuts.has_key('exlarge16to9'):
                return cuts['exlarge16to9'].url
        else:
            return None
    
    # Assemble feed URL to use at this point in time
    def _get_CNN_feed_url():
        return "https://services.cnn.com/newsgraph/search/type:video/firstPublishDate:2015-10-29T00:00:00Z~2015-10-29T23:59:59Z/rows:50/start:0/sort:lastPublishDate,desc?api_key=c2vfn5fb8gubhrmd67x7bmv9"

    @tornado.gen.coroutine
    def lookup_and_submit_videos(self, ovp_video_ids, continue_on_error=False):
        '''Looks up a list of video ids and submits them.

        Returns: dictionary of video_id => job_id or exception
        '''
        try:
            bc_video_info = yield self.bc_api.find_videos_by_ids(
                ovp_video_ids,
                video_fields=BrightcoveIntegration.get_submit_video_fields(),
                custom_fields=self.get_custom_fields(),
                async=True)
        except brightcove_api.BrightcoveApiServerError as e:
            statemon.state.increment('bc_apiserver_errors')
            _log.error('Server error getting data from Brightcove for '
                       'platform %s: %s' % (self.platform.get_id(), e))
            raise integrations.ovp.OVPError(e)
        except brightcove_api.BrightcoveApiClientError as e:
            statemon.state.increment('bc_apiclient_errors')
            _log.error('Client error getting data from Brightcove for '
                       'platform %s: %s' % (self.platform.get_id(), e))
            raise integrations.ovp.OVPError(e)
            

        retval = yield self.submit_many_videos(
            bc_video_info,
            continue_on_error=continue_on_error)

        raise tornado.gen.Return(retval)



    @tornado.gen.coroutine
    def process_publisher_stream(self):
        yield self.lookup_cnn_new_videos()


    def lookup_cnn_new_videos():
        thumb = ""
        vid_src = ""
        videoID = ""

        request_url = _get_CNN_feed_url()
        data = _make_CNN_feed_request(request_url)

        if data.has_key('docs'):
            for video in data['docs']:
                # TODO add check to make sure the video doesn't already exist on our side

                thumb = _get_best_image_info(data['relatedMedia'])
                vid_src = _get_video_url_to_download(data)
                videoID = _get_videoID(data)
                if (thumb != "" and vid_src != "" and videoID != ""):
                    submit_one_video_object(videoID, thumb, vid_src)

    @tornado.gen.coroutine
    def _submit_one_video_object_impl(self, vid_obj, grab_new_thumb=True,
                                      skip_old_video=False):
        # Get the video url to process
        video_url = self._get_video_url_to_download(vid_obj)
        if (video_url is None or 
            vid_obj['length'] < 0 or 
            video_url.endswith('.m3u8') or 
            video_url.startswith('rtmp://') or 
            video_url.endswith('.csmil')):
            _log.warn_n('Brightcove id %s for account %s is a live stream' 
                        % (vid_obj['id'], self.platform.neon_api_key))
            raise tornado.gen.Return(None)

        # Get the thumbnail attached to the video
        thumb_url, thumb_data = \
              BrightcoveIntegration._get_best_image_info(vid_obj)
        thumb_id = None
        if thumb_data is not None:
            thumb_id = unicode(thumb_data['id'])

        # Build up the custom data we are going to store in our database.
        # TODO: Determine all the information we want to grab and store
        custom_data = vid_obj.get('customFields', {})
        custom_data['_bc_int_data'] = {
            'bc_id' : vid_obj['id'],
            'bc_refid' : vid_obj.get('referenceId', None)
            }

        # Get the video id to use to key this video
        if self.platform.id_field == neondata.BrightcovePlatform.REFERENCE_ID:
            video_id = vid_obj.get('referenceId', None)
            if video_id is None:
                msg = ('No valid reference id in video %s for account %s'
                       % (vid_obj['id'], self.platform.neon_api_key))
                statemon.state.increment('cant_get_refid')
                _log.error_n(msg)
                raise integrations.ovp.OVPError(msg)
        elif (self.platform.id_field == 
              neondata.BrightcovePlatform.BRIGHTCOVE_ID):
            video_id = vid_obj['id']
        else:
            # It's a custom field, so look for it
            video_id = custom_data.get(self.platform.id_field, None)
            if video_id is None:
                msg = ('No valid id in custom field %s in video %s for '
                       'account %s' %
                       (self.platform.id_field, vid_obj['id'],
                        self.platform.neon_api_key))
                _log.error_n(msg)
                statemon.state.increment('cant_get_custom_id')
                raise integrations.ovp.OVPError(msg)
        video_id = unicode(video_id)

        # Get the published date
        publish_date = vid_obj.get('publishedDate', None)
        if publish_date is not None:
            publish_date = datetime.datetime.utcfromtimestamp(
                int(publish_date) / 1000.0)

        if not video_id in self.platform.videos:
            # See if the video should be skipped because it is too old
            if (skip_old_video and 
                publish_date is not None and 
                self.platform.oldest_video_allowed is not None and
                publish_date < 
                dateutil.parser.parse(self.platform.oldest_video_allowed)):
                _log.info('Skipped video %s from account %s because it is too'
                          ' old' % (video_id, self.platform.neon_api_key))
                statemon.state.increment('old_videos_skipped')
                raise tornado.gen.Return(None)
        
            # The video hasn't been submitted before
            job_id = None
            try:
                response = yield self.submit_video(
                    video_id,
                    video_url,
                    video_title=unicode(vid_obj['name']),
                    default_thumbnail=thumb_url,
                    external_thumbnail_id=thumb_id,
                    callback_url=self.platform.callback_url,
                    custom_data = custom_data,
                    duration=float(vid_obj['length']) / 1000.0,
                    publish_date=(publish_date.isoformat() if 
                                  publish_date is not None else None))
                job_id = response['job_id']

            except Exception as e:
                # If the video metadata object is there, then try to
                # find the job id in that oject.
                new_video = yield tornado.gen.Task(
                    neondata.VideoMetadata.get,
                    neondata.InternalVideoID.generate(
                        self.platform.neon_api_key, video_id))
                if new_video is not None:
                    job_id = new_video.job_id
                raise e

            finally:
                # TODO: Remove this hack once videos aren't attached to
                # platform objects.
                # HACK: Add the video to the platform object because our call 
                # will put it on the NeonPlatform object.
                if job_id is not None:
                    self.platform = yield tornado.gen.Task(
                        neondata.BrightcovePlatform.modify,
                        self.platform.neon_api_key,
                        self.platform.integration_id,
                    lambda x: x.add_video(video_id, job_id))


        else:
            job_id = self.platform.videos[video_id]
            if job_id is not None:
                yield self._update_video_info(vid_obj, video_id, job_id)
            if grab_new_thumb:
                yield self._grab_new_thumb(vid_obj, video_id)

        raise tornado.gen.Return(job_id)

 