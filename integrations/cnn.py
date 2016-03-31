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
import logging
import urlparse
import re
import time
import tornado.gen
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
        and associate our video object to it.
        '''

        # Pull out the url, associated data for the thumbnail
        thumb_url, thumb_data = self._get_best_image_info(data)
        if thumb_data is None:
            _log.warn_n('Could not find thumbnail in %s' % data)
            return
        ext_thumb_urls = [self.normalize_thumbnail_url(x)
                          for x in self._(data)]
        # has keys
        # thumb url, e.g.,
        # http://i2.cdn.turner.com/money/dam/assets/160301111110-donald-trump-campaigning-1100x619.jpg
        # thumb ref, e.g.,
        # h_0ba171f3e9fe3671fe5917d1fd689dd8 or None

        # Find our objects that corresponds to CNN's id
        video_meta = yield neondata.VideoMetadata.get_by_external_id(
            self.platform.neon_api_key, external_video_id)
        if not video_meta:
            _log.error('Could not find video %s' % external_video_id)
            statemon.state.increment('video_not_found')
            return
        thumbs_meta = yield tornado.gen.Task(neondata.ThumbnailMetadata.get_many,
                                             video_meta.thumbnail_ids)

        # Function that will set the external id in the ThumbnailMetadata
        external_id = thumb_data.get('id', None)
        if external_id is not None:
            external_id = unicode(external_id)

        def _set_external_id(obj):
            obj.external_id = external_id

        found_thumb, min_rank = False, 1
        for thumb in thumbs_meta:
            # BC conditional
            if (thumb is None or thumb.type != neondata.ThumbnailType.DEFAULT):
                continue

            if thumb.rank < min_rank:
                min_rank = thumb.rank

            # We know about this thumb was in BC so see if it is still there.
            response_ids = self._get_media_field_from_response(data, 'id')
            if thumb.external_id is not None:
                if unicode(thumb.external_id) in response_ids:
                    found_thumb = True

            # For legacy thumbs, we specified a reference id. Look for it
            elif thumb.refid is not None:
                if thumb.refid in self._get_media_urls_from_response(
                        data, 'referenceId'):
                    found_thumb = True
                    yield tornado.gen.Task(neondata.ThumbnailMetadata.modify,
                                           thumb.key,
                                           _set_external_id)

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

        is_exist = False
        if not found_thumb:
            # Add the thumbnail to our system
            urls = self._get_media_urls_from_response(data)
            added_image = False
            for url in urls[::-1]:
                try:
                    # Add a new thumbnail of type default, with min rank.
                    new_thumb = neondata.ThumbnailMetadata(
                        None,
                        ttype=neondata.ThumbnailType.DEFAULT,
                        rank=min_rank - 1,
                        external_id=external_id
                    )
                    yield video_meta.download_and_add_thumbnail(
                        new_thumb,
                        url,
                        save_objects=False,
                        async=True)

                    # Validate the new_thumb key exists or not. We noticed that
                    # Brightcove can send the same thumbnail with different
                    # external ids multiple times. This triggers the same
                    # thumbnail as new and restart the experiment. Since the
                    # thumbnail key is generated by md5 hashing, we will compare
                    # the hash with existing thumbnail hashes, if find a match
                    # we will disgard the thumbnail.
                    is_exist = \
                        any([new_thumb.key == old_thumb.key
                            for old_thumb in thumbs_meta]) or \
                        any([new_thumb.phash == old_thumb.phash
                            for old_thumb in thumbs_meta])

                    if is_exist:
                        continue

                    sucess = yield tornado.gen.Task(new_thumb.save)
                    if not sucess:
                        raise IOError('Could not save thumbnail')
                    # Even though the video_meta already has the new_thumb
                    # in thumbnail_ids, the database still doesn't have it yet.
                    # We will modify in database first. Also, modify is used
                    # first instead of using save directively, as other process
                    # can modify the video_meta as well.
                    updated_video = yield tornado.gen.Task(
                        video_meta.modify,
                        video_meta.key,
                        lambda x: x.thumbnail_ids.append(new_thumb.key))
                    if updated_video is None:
                        # It wasn't in the database, so save this object
                        sucess = yield tornado.gen.Task(video_meta.save)
                        if not sucess:
                            raise IOError('Could not save video data')
                    else:
                        video_meta.__dict__ = updated_video.__dict__

                    _log.info(
                        'Found new thumbnail %s for video %s at Brigthcove.' %
                        (external_id, video_meta.key))
                    statemon.state.increment('new_images_found')
                    added_image = True
                    break
                except IOError:
                    # Error getting the image, so keep going
                    pass
            if not added_image and not is_exist:
                _log.error('Could not find valid image to add to video %s. '
                           'Tried urls %s' % (video_meta.key, urls))
                statemon.state.increment('cant_get_image')

        # There's a ton of code in the BC version of this that looks factorable
        # TODO Check if db is capturing value of ext id and add it if not
        pass

    @staticmethod
    def _normalize_thumbnail_url(url):
        '''Returns a thumb url without transport mechanism or query string.'''
        if url is None:
            return None
        parse = urlparse.urlparse(url)
        return '%s%s' % (parse.netloc, parse.path)

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
        return self._build_custom_data_from_topics(video['topics'])

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

           @TODO why is the first image in CNN's list the best?
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
    def _build_custom_data_from_topics(topics):
        custom_data = {}
        custom_data['topics'] = []
        for t in topics:
            custom_data['topics'].append(t)
        return custom_data

    @staticmethod
    def _get_media_field_from_response(response, field):
        '''Extract a list of fields from the images in the response.'''
        media = response['relatedMedia']['media']
        return [unicode(m.get(field)) for m in media if m.get(field) is not None]

    @staticmethod
    def _get_media_urls_from_response(response):
        '''Get a list of urls, one for each item in media'''
        media = response['relatedMedia']['media']
        return [m.cuts['exlarge16to9'] for m in media]

    @tornado.gen.coroutine
    def process_publisher_stream(self):
        yield self.submit_new_videos()
