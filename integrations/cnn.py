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
import datetime
import dateutil.parser
import logging
import re
import time
import urlparse
import tornado.gen
import api.cnn_api
import integrations.ovp
from cmsdb import neondata
from utils import statemon

statemon.define('unexpected_submission_error', int)

_log = logging.getLogger(__name__)


class CNNIntegration(integrations.ovp.OVPIntegration):

    def __init__(self, account_id, integration):
        super(CNNIntegration, self).__init__(account_id, integration)

        self.account_id = account_id
        self.api = api.cnn_api.CNNApi(integration.api_key_ref)
        self.last_process_date = integration.last_process_date
        # to correspond with the inherited class
        self.platform = integration
        self.neon_api_key = account_id

    def _grab_new_thumb(self, data, external_video_id):
        '''Grab a thumb from CNN's video data if a new one exists
        and associate our video object to it as a default image.
        '''

        # Pull out the url, associated data for the thumbnail
        thumb_url, thumb_data = self._get_best_image_info(data)
        if thumb_data is None:
            _log.warn_n('Could not find thumbnail in %s' % data)
            return
        ext_thumb_urls = [self.normalize_thumbnail_url(x)
                          for x in self._(data)]

        # Find our objects that corresponds to CNN's id
        video_meta = yield neondata.VideoMetadata.get_by_external_id(
            self.platform.neon_api_key, external_video_id)
        if not video_meta:
            _log.error('Could not find video %s' % external_video_id)
            statemon.state.increment('video_not_found')
            return
        thumbs_meta = yield tornado.gen.Task(neondata.ThumbnailMetadata.get_many,
                                             video_meta.thumbnail_ids)

        external_id = thumb_data.get('id', None)
        if external_id is not None:
            external_id = unicode(external_id)

        # Function that will set the external id in the ThumbnailMetadata
        def _set_external_id(obj):
            obj.external_id = external_id

        found_thumb, min_rank = False, 1
        for thumb in thumbs_meta:

            if (thumb is None or thumb.type != neondata.ThumbnailType.DEFAULT):
                continue

            if thumb.rank < min_rank:
                min_rank = thumb.rank

            # Check if our record's external id matches the response's
            response_ids = self._get_image_field_from_response(data, 'id')
            if thumb.external_id is not None:
                if unicode(thumb.external_id) in response_ids:
                    found_thumb = True

            else:
                # We do not have the id for this thumb, so see if we
                # can match the url.
                norm_urls = set([self.normalize_thumbnail_url(x)
                                 for x in thumb.urls])
                if len(norm_urls.intersection(ext_thumb_urls)) > 0:
                    found_thumb = True

                    # Now update the external id because we didn't
                    # know about it before.
                    yield tornado.gen.Task(neondata.ThumbnailMetadata.modify,
                                           thumb.key,
                                           _set_external_id)

        # The thumb is not found, so add it to our records
        # (provided it doesn't have a duplicate hash).
        if not found_thumb:
            self._add_default_thumbnail(data, video_meta, thumbs_meta,
                                        external_id, min_rank=min_rank)

    @tornado.gen.coroutine
    def process_publisher_stream(self):
        yield self.submit_new_videos()

    @tornado.gen.coroutine
    def submit_new_videos(self):
        acct = yield tornado.gen.Task(neondata.NeonUserAccount.get, self.account_id)
        self.neon_api_key = acct.neon_api_key
        self.account_id = acct.account_id
        search_results = yield self.api.search(
            dateutil.parser.parse(self.last_process_date)
        )
        videos = search_results['docs']
        _log.info('Processing %d videos for cnn' % (len(videos)))
        self.set_video_iter(videos)

        yield self.submit_ovp_videos(self.get_next_video_item, grab_new_thumb=True)

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
        topics = video['topics']
        custom_data = {}
        custom_data['topics'] = []
        for t in topics:
            custom_data['topics'].append(t)
        return custom_data

    def get_video_duration(self, video):
        '''override from ovp'''
        duration = video.get('duration', None)
        if duration:
            ts = time.strptime(duration, '%H:%M:%S')
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
        return {
            'thumb_url': thumb_url,
            'thumb_ref': thumb_ref
        }

    def set_video_iter(self, videos):
        self.video_iter = iter(videos)

    def does_video_exist(self, video_meta, video_ref):
        return video_meta is not None

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
           CNN related media object.
        '''
        if 'media' in media_json:
            # now we need to iter on the items as there could be multiple images here
            for item in media_json['media']:
                try:
                    if item['type'] == 'image':
                        cuts = item['cuts']
                        thumb_id = item['imageId']
                        if 'exlarge16to9' in cuts:
                            return cuts['exlarge16to9']['url'], thumb_id
                except KeyError:
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
            raise integrations.ovp.OVPNoValidURL(
                'unable to find a suitable url for processing'
            )

    @staticmethod
    def _get_image_field_from_response(response, field):
        '''Extract a list of fields from the media in the response.'''
        media = response['relatedMedia']['media']
        return [unicode(m.get(field)) for m in media if m.get(field) is not None]

    @staticmethod
    def _get_image_urls_from_response(response):
        '''Get a list of urls, one for each item in media'''
        media = response['relatedMedia']['media']
        return [m.cuts['exlarge16to9'] for m in media]

    @staticmethod
    def _normalize_thumbnail_url(url):
        '''Returns a thumb url without transport mechanism or query string.'''
        if url is None:
            return None
        parse = urlparse.urlparse(url)
        return '%s%s' % (parse.netloc, parse.path)
