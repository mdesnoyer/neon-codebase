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
import datetime
import dateutil.parser
import logging
import re
import urlparse
import integrations.ovp
import tornado.gen
from api import brightcove_api
from cmsdb import neondata
from utils import statemon
from utils.options import define, options

define('max_vids_for_new_account', default=100,
       help='Maximum videos to process for a new account')

define('max_submit_retries', default=3,
       help='Maximum times we will retry a video submit before passing on it.')

define('bc_servingurl_push_callback_host', default=None,
       help='Location to send the callback to push out a serving url')

statemon.define('bc_apiserver_errors', int)
statemon.define('bc_apiclient_errors', int)
statemon.define('cant_get_refid', int)
statemon.define('cant_get_custom_id', int)
statemon.define('old_videos_skipped', int)

_log = logging.getLogger(__name__)


class BrightcoveIntegration(integrations.ovp.OVPIntegration):
    def __new__(cls, account_id, platform):
        # Figure out if we have permissions to talk to the media api
        # or the cms api and build the correct object as a result
        if cls is BrightcoveIntegration:
            if (platform.application_client_id is not None and 
                platform.application_client_secret is not None):
                return super(BrightcoveIntegration, cls).__new__(
                    CMSAPIIntegration)
            else:
                return super(BrightcoveIntegration, cls).__new__(
                    MediaAPIIntegration)
            raise ValueError('No valid authentication tokens found')
        else:
            return super(BrightcoveIntegration, cls).__new__(
                cls, account_id, platform)

    def get_video_id(self, video):
        '''override from ovp'''
        # Get the video id to use to key this video
        if self.platform.id_field == neondata.BrightcoveIntegration.REFERENCE_ID:
            video_id = self.get_reference_id(video)
            if video_id is None:
                msg = ('No valid reference id in video %s for account %s'
                       % (video['id'], self.neon_api_key))
                statemon.state.increment('cant_get_refid')
                _log.error_n(msg)
                raise integrations.ovp.OVPRefIDError(msg)
        elif (self.platform.id_field == 
              neondata.BrightcoveIntegration.BRIGHTCOVE_ID):
            video_id = video['id']
        else:
            # It's a custom field, so look for it
            custom_data = self.get_video_custom_data(video)
            video_id = custom_data.get(self.platform.id_field, None)
            if video_id is None:
                msg = ('No valid id in custom field %s in video %s for '
                       'account %s' %
                       (self.platform.id_field, video['id'],
                        self.neon_api_key))
                _log.error_n(msg)
                statemon.state.increment('cant_get_custom_id')
                raise integrations.ovp.OVPCustomRefIDError(msg)
        return video_id

    def get_video_callback_url(self, video):
        '''override from ovp'''
        if self.platform.callback_url:
            return self.platform.callback_url
        elif options.bc_servingurl_push_callback_host:
            return ('http://%s/update_serving_url/%s' %
                    (options.bc_servingurl_push_callback_host,
                     self.platform.integration_id))
        return None

    @staticmethod
    def get_video_title(video):
        '''override from ovp'''
        video_title = video.get('name', '')
        return unicode(video_title)

    @staticmethod
    def _normalize_thumbnail_url(url):
        '''Returns a thumb url without transport mechanism or query string,
        with specific logic for brightcove's domain naming.
        '''

        if url is None:
            return None

        parse = urlparse.urlparse(url)
        # Brightcove can move the image around, but its basename will
        # remain the same, so if it is a brightcove url, only look at
        # the basename.
        if re.compile('(brightcove)|(bcsecure)').search(parse.netloc):
            return 'brightcove.com/%s' % (os.path.basename(parse.path))
        return '%s%s' % (parse.netloc, parse.path)

    def _log_statemon_submit_video_error(self):
        statemon.state.define_and_increment(
            'submit_video_bc_error.%s' % self.account_id)

    def does_video_exist(self, video_meta, video_ref): 
        return video_meta is not None
 
class CMSAPIIntegration(BrightcoveIntegration):
    def __init__(self, account_id, platform):
        super(CMSAPIIntegration, self).__init__(account_id, platform)
        self.bc_api = brightcove_api.CMSAPI(
            platform.publisher_id,
            platform.application_client_id,
            platform.application_client_secret)
        self.neon_api_key = self.account_id = account_id
        self.cur_video_sources = None

    def get_reference_id(self, video):
        return video.get('reference_id')

    def get_video_duration(self, video):
        return float(video['duration']) / 1000.0

    def get_video_url(self, video):
        video_srcs = []  # (width, encoding_rate, url)
        for source in self.cur_video_sources: 
            src = source.get('src', None)
            if src is not None:
                video_srcs.append((
                    (source.get('width',-1)),
                    (source.get('encoding_rate',-1)),
                    src))
            
        if len(video_srcs) < 1:
            _log.error("Could not find url to download : %s" % video)
            return None

        if self.platform.rendition_frame_width:
            # Get the highest encoding rate at a size closest to this width
            video_srcs = sorted(
                video_srcs, key=lambda x:
                (abs(x[0] - self.platform.rendition_frame_width),
                -x[1]))
        else:
            # return the max width rendition with the highest encoding rate
            video_srcs = sorted(video_srcs, key=lambda x: (-x[0], -x[1]))
        return video_srcs[0][2]

    def get_video_custom_data(self, video):
        custom_data = video.get('custom_fields', {})
        custom_data['_bc_int_data'] = {
            'bc_id': video['id'],
            'bc_refid': self.get_reference_id(video)
        }
        return custom_data

    @tornado.gen.coroutine
    def process_publisher_stream(self):
        yield self.submit_new_videos()

    def get_video_publish_date(self, video):
        return video['published_at']

    def get_video_last_modified_date(self, video):
        return video['updated_at']

    def get_video_thumbnail_info(self, video):
        """get the poster image if its available 
             if not grab the thumbnail
             else return None

        """
        thumb_url, thumb_ref = self._get_best_image_info(video)

        if not thumb_ref or not thumb_url: 
            _log.warning('Unable to find image info for video %s' % video)
        else: 
            thumb_ref = unicode(thumb_ref['id'])

        return {'thumb_url': thumb_url,
                'thumb_ref': thumb_ref}

    @tornado.gen.coroutine
    def set_video_iter(self):
        from_date_str = datetime.datetime.now().strftime(
            '%Y-%m-%dT%H:%M:%SZ')
        if (self.platform.last_process_date is not None): 
            from_date_str = self.platform.last_process_date

        # possible TODO, this could use the feed iterator 
        # and help catch up videos faster, however in our 
        # new limits based implementation this isn't necessary
        # at the moment, just get the most recent 30 vids
        try: 
            videos = yield self.bc_api.get_videos(
                limit=30,
                q='updated_at:%s' % from_date_str)
            self.video_iter = iter(videos)
        except (brightcove_api.BrightcoveApiServerError,
                brightcove_api.BrightcoveApiClientError,
                brightcove_api.BrightcoveApiNotAuthorizedError,
                brightcove_api.BrightcoveApiError) as e:
            _log.error('Brightcove Error occurred trying to get videos : %s' % e)
            self.video_iter = iter([]) 
            pass  


    @tornado.gen.coroutine
    def get_next_video_item(self):
        video = None
        try:
            video = self.video_iter.next()
            self.cur_video_sources = yield self.bc_api.get_video_sources(
                video['id'])
        except StopIteration:
            video = StopIteration('hacky')

        raise tornado.gen.Return(video)

    @tornado.gen.coroutine
    def submit_new_videos(self):
        '''Submits new videos in the account.'''
        yield self.set_video_iter()
        yield self.submit_ovp_videos(self.get_next_video_item)

    @staticmethod
    def _get_best_image_info(video):
        '''Returns the (url, {image_struct}) of the best image in the
        Brightcove video object
        '''
        images = video.get('images', None) 
        if not images: 
            _log.error('Unable to find images for video %s' % video)
            return None 
 
        thumb_url = None 
        thumb_ref = None
 
        poster_image = images.get('poster', None) 
        thumbnail_image = images.get('thumbnail', None) 

        if poster_image: 
            thumb_ref = poster_image.get('asset_id', None) 
            thumb_url = poster_image.get('src', None) 
        elif thumbnail_image:
            thumb_ref = thumbnail_image.get('asset_id', None)
            thumb_url = thumbnail_image.get('src', None)
        
        if not thumb_ref or not thumb_url: 
            _log.warning('Unable to find image info for video %s' % video)
            return None, {'id': None}
        else: 
            thumb_ref = unicode(thumb_ref)

        return thumb_url, {'id': thumb_ref} 
 
    @staticmethod
    def _extract_image_field(response, field):
        '''Extract values of a field in the images in the response
           from Brightcove

           Return list of unicode strings
        '''
        vals = []
        images = response['images']
        # TODO clean this up, possibly remove generic from ovp
        if field == 'id': 
            field = 'asset_id' 
 
        for image_type in ['poster', 'thumbnail']:
            fields = images.get(image_type, None)
            if fields is not None:
                vals.append(fields.get(field, None))

        return [unicode(x) for x in vals if x is not None]

    @staticmethod
    def _extract_image_urls(response):
        '''Extract the list of image urls in the response from Brightcove'''
        urls = []
        images = response.get('images', None) 
        
        for image_type in ['poster', 'thumbnail']:
            image = images.get(image_type, None) 
            urls.append(image.get('src', None))

        return [x for x in urls if x is not None]

class MediaAPIIntegration(BrightcoveIntegration):
    def __init__(self, account_id, platform):
        super(MediaAPIIntegration, self).__init__(account_id, platform)
        self.bc_api = brightcove_api.BrightcoveApi(
            account_id, self.platform.publisher_id,
            self.platform.read_token, self.platform.write_token)
        self.skip_old_videos = False
        self.neon_api_key = self.account_id = account_id

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
                'publishedDate',
                'lastModifiedDate',
                'referenceId']

    def get_custom_fields(self):
        '''Return a list of custom fields to request.'''
        custom_fields = None
        if self.platform.id_field not in [
                neondata.BrightcoveIntegration.REFERENCE_ID, 
                neondata.BrightcoveIntegration.BRIGHTCOVE_ID]:
            custom_fields = [self.platform.id_field]

        return custom_fields

    def _get_video_url_to_download(self, b_json_item):
        '''
        Return a video url to download from a brightcove json item.

        If frame_width is specified, get the closest one.
        '''

        video_urls = []  # (width, encoding_rate, url)
        try:
            d_url = b_json_item['FLVURL']
        except KeyError:
            _log.error("missing flvurl %s" % b_json_item)
            return None

        # If we get a broken response from brightcove api
        if 'renditions' not in b_json_item:
            return d_url

        renditions = b_json_item['renditions']
        for rend in renditions:
            url = rend.get("url", None)
            if url is not None:
                video_urls.append(((rend["frameWidth"] or -1),
                                   (rend["encodingRate"] or -1),
                                  url))
            elif rend['remoteUrl'] is not None:
                video_urls.append(((rend["frameWidth"] or -1),
                                   (rend["encodingRate"] or -1),
                                  rend['remoteUrl']))

        # no renditions
        if len(video_urls) < 1:
            return d_url

        if self.platform.rendition_frame_width:
            # Get the highest encoding rate at a size closest to this width
            video_urls = sorted(video_urls, key=lambda x:
                                (abs(x[0] - self.platform.rendition_frame_width),
                                 -x[1]))
        else:
            # return the max width rendition with the highest encoding rate
            video_urls = sorted(video_urls, key=lambda x: (-x[0], -x[1]))
        return video_urls[0][2]

    @tornado.gen.coroutine
    def lookup_and_submit_videos(self, ovp_video_ids, continue_on_error=False):
        '''Looks up a list of video ids and submits them.

        Returns: dictionary of video_id => job_id or exception
        '''
        try:
            bc_video_info = yield self.bc_api.find_videos_by_ids(
                ovp_video_ids,
                video_fields=self.get_submit_video_fields(),
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

        self.set_video_iter_with_videos(bc_video_info)
        retval = yield self.submit_ovp_videos(self.get_next_video_item_playlist,
                                              continue_on_error=continue_on_error)

        raise tornado.gen.Return(retval)

    @tornado.gen.coroutine
    def set_thumbnail(self, ovp_id, thumb_metadata):
        '''Sets the Brightcove thumbnail that is returned from their apis.'''
        # Not implemented for now because it's not needed
        raise NotImplementedError()

        # This will use update_thumbnail_and_videostill in brightcove
        # api and make changes to our database as necessary.

    @tornado.gen.coroutine
    def submit_playlist_videos(self):
        '''Submits any playlist videos for this account.

        Returns: dictionary of video_id => job_id
        '''
        retval = {}
        for playlist_id in self.platform.playlist_feed_ids:
            try:
                cur_results = yield self.bc_api.find_playlist_by_id(
                    playlist_id,
                    video_fields=self.get_submit_video_fields(),
                    playlist_fields=['id', 'videos'],
                    custom_fields=self.get_custom_fields(),
                    async=True)
            except brightcove_api.BrightcoveApiServerError as e:
                statemon.state.increment('bc_apiserver_errors')
                _log.error('Server error getting playlist %s from '
                           'Brightcove for platform %s: %s' %
                           (playlist_id, self.platform.get_id(), e))
                raise integrations.ovp.OVPError(e)
            except brightcove_api.BrightcoveApiClientError as e:
                statemon.state.increment('bc_apiclient_errors')
                _log.error('Client error getting playlist %s from '
                           'Brightcove for platform %s: %s' %
                           (playlist_id, self.platform.get_id(), e))
                raise integrations.ovp.OVPError(e)

            self.set_video_iter_with_videos(cur_results['videos'])
            cur_jobs = yield self.submit_ovp_videos(self.get_next_video_item_playlist)
            retval.update(cur_jobs)

        raise tornado.gen.Return(retval)

    @tornado.gen.coroutine
    def submit_new_videos(self):
        '''Submits new videos in the account.'''
        self.skip_old_videos = True
        self.set_video_iter()
        yield self.submit_ovp_videos(self.get_next_video_item_normal)

    @tornado.gen.coroutine
    def process_publisher_stream(self):
        yield self.submit_playlist_videos()
        yield self.submit_new_videos()

    def get_reference_id(self, video):
        return video.get('referenceId', None)

    def get_video_url(self, video):
        '''override from ovp'''
        return self._get_video_url_to_download(video)

    def get_video_custom_data(self, video):
        '''override from ovp'''
        custom_data = video.get('customFields', {})
        custom_data['_bc_int_data'] = {
            'bc_id': video['id'],
            'bc_refid': video.get('referenceId', None)
        }
        return custom_data

    def get_video_duration(self, video):
        '''override from ovp'''
        return float(video['length']) / 1000.0

    def get_video_publish_date(self, video):
        '''override from ovp'''
        publish_date = video.get('publishedDate', None)
        if publish_date is not None:
            publish_date = datetime.datetime.utcfromtimestamp(
                int(publish_date) / 1000.0)
        return publish_date

    def get_video_last_modified_date(self, video):
        try:
            return int(video['lastModifiedDate']) / 1000.0
        except TypeError:
            return None

    def get_video_thumbnail_info(self, video):
        '''override from ovp'''
        thumb_url, thumb_data = self._get_best_image_info(video)
        thumb_ref = None
        if thumb_data is not None:
            thumb_ref = unicode(thumb_data['id'])
        return {'thumb_url': thumb_url,
                'thumb_ref': thumb_ref}

    def set_video_iter(self):
        from_date = datetime.datetime(1980, 1, 1)
        if (self.platform.last_process_date is not None and
                not self.platform.uses_batch_provisioning):
            from_date = datetime.datetime.utcfromtimestamp(
                self.platform.last_process_date)

        self.video_iter = self.bc_api.find_modified_videos_iter(
                from_date=from_date,
                _filter=['UNSCHEDULED', 'INACTIVE', 'PLAYABLE'],
                sort_by='MODIFIED_DATE',
                sort_order='ASC',
                video_fields=self.get_submit_video_fields(),
                custom_fields=self.get_custom_fields())

    def set_video_iter_with_videos(self, videos):
        self.video_iter = iter(videos)

    @tornado.gen.coroutine
    def get_next_video_item_normal(self):
        try:
            item = yield self.video_iter.next(async=True)
            if isinstance(item, StopIteration):
                item = StopIteration('hacky')
        except brightcove_api.BrightcoveApiServerError as e:
            statemon.state.increment('bc_apiserver_errors')
            _log.error('Server error getting new videos from '
                       'Brightcove for platform %s: %s' %
                       (self.platform.get_id(), e))
            raise integrations.ovp.OVPError(e)
        except brightcove_api.BrightcoveApiClientError as e:
            statemon.state.increment('bc_apiclient_errors')
            _log.error('Client error getting new videos from '
                       'Brightcove for platform %s: %s' %
                       (self.platform.get_id(), e))
            raise integrations.ovp.OVPError(e)

        raise tornado.gen.Return(item)

    @tornado.gen.coroutine
    def get_next_video_item_playlist(self):
        video = None
        try:
            video = self.video_iter.next()
        except StopIteration:
            video = StopIteration('hacky')

        raise tornado.gen.Return(video)

    def skip_old_video(self, publish_date, video_id):
        rv = False
        if (self.skip_old_videos and
            publish_date is not None and
            self.platform.oldest_video_allowed is not None and
            publish_date <
            dateutil.parser.parse(self.platform.oldest_video_allowed)):
            _log.info('Skipped video %s from account %s because it is too'
                      ' old' % (video_id, self.neon_api_key))
            statemon.state.increment('old_videos_skipped')
            rv = True
        return rv

    @staticmethod
    def _get_best_image_info(data):
        '''Returns the (url, {image_struct}) of the best image in the
        Brightcove video object
        '''
        url = data.get('videoStillURL', None)
        if url is None:
            url = data.get('thumbnailURL', None)
            if url is None:
                return (None, None)
            obj = data['thumbnail']
        else:
            obj = data['videoStill']

        return url, obj

    @staticmethod
    def _extract_image_field(response, field):
        '''Extract values of a field in the images in the response
           from Brightcove

           Return list of unicode strings
        '''
        vals = []
        for image_type in ['thumbnail', 'videoStill']:
            fields = response.get(image_type, None)
            if fields is not None:
                vals.append(fields.get(field, None))

        return [unicode(x) for x in vals if x is not None]

    @staticmethod
    def _extract_image_urls(response):
        '''Extract the list of image urls in the response from Brightcove'''
        urls = []
        for image_type in ['thumbnail', 'videoStill']:
            urls.append(response.get(image_type + 'URL', None))
            if response.get(image_type, None) is not None:
                urls.append(response[image_type].get('remoteUrl', None))

        return [x for x in urls if x is not None]
