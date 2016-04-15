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
import datetime
import dateutil.parser
import integrations.ovp
import logging
import re
import time
import tornado.gen
import urlparse
from cmsdb import neondata

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

    @staticmethod
    def get_video_title(video):
        '''override from ovp'''
        if video.get('title') is not None:
            return unicode(video['title'])
        if video.get('headline') is not None:
            return unicode(video['headline'])
        return u'no title'

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
        thumb_url, thumb_obj = self._get_best_image_info(video)
        return {
            'thumb_url': thumb_url,
            'thumb_ref': thumb_obj['id']
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
        if 'relatedMedia' in media_json and 'media' in media_json['relatedMedia']:
            # now we need to iter on the items as there could be multiple images here
            for item in media_json['relatedMedia']['media']:
                try:
                    if item['type'] == 'image':
                        cuts = item['cuts']
                        if item.get('id') is not None:
                            thumb_id = item['id']
                        elif item.get('imageId') is not None:
                            thumb_id = item['imageId']
                        if 'exlarge16to9' in cuts:
                            return cuts['exlarge16to9']['url'], {'id': thumb_id}
                except KeyError:
                    continue
        else:
            return None, {'id': None}

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
    def _extract_image_field(response, field):
        '''Extract values of a field in the images in the response from CNN

           Return list of unicode strings
        '''
        media = response['relatedMedia']['media']
        return [unicode(m.get(field)) for m in media
                if m['type'] == 'image' and
                m.get(field) is not None]

    @staticmethod
    def _extract_image_urls(response):
        '''Extract the list of high-quality image urls in the response
           from CNN'''
        media = response['relatedMedia']['media']
        return [m['cuts'].get('exlarge16to9')['url'] for m in media
                if m['type'] == 'image' and
                m['cuts'].get('exlarge16to9') is not None]

    @staticmethod
    def _normalize_thumbnail_url(url):
        '''Returns a thumb url without transport mechanism or query string.'''
        if url is None:
            return None
        parse = urlparse.urlparse(url)
        return '%s%s' % (parse.netloc, parse.path)
