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
import datetime
import dateutil.parser
import integrations.ovp
import logging
import re
import tornado.gen
import urlparse
from utils.options import options, define
from utils import statemon

define('max_vids_for_new_account', default=100, 
       help='Maximum videos to process for a new account')

statemon.define('bc_apiserver_errors', int)
statemon.define('bc_apiclient_errors', int)
statemon.define('unexpected_submition_error', int)
statemon.define('new_images_found', int)
statemon.define('cant_get_image', int)
statemon.define('cant_get_refid', int)
statemon.define('cant_get_custom_id', int)
statemon.define('video_not_found', int)
statemon.define('old_videos_skipped', int)

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

def _extract_image_info_from_bc_response(response, field):
    '''Extracts a list of fields from the images in the response.'''
    vals = []
    for image_type in ['thumbnail', 'videoStill']:
        fields = response.get(image_type, None)
        if fields is not None:
            vals.append(fields.get(field, None))

    return [unicode(x) for x in vals if x is not None]

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
                'publishedDate',
                'lastModifiedDate',
                'referenceId']

    def get_custom_fields(self):
        '''Return a list of custom fields to request.'''
        custom_fields = None
        if self.platform.id_field not in [
                neondata.BrightcovePlatform.REFERENCE_ID, 
                neondata.BrightcovePlatform.BRIGHTCOVE_ID]:
            custom_fields = [self.platform.id_field]

        return custom_fields

    def _get_video_url_to_download(self, b_json_item):
        '''
        Return a video url to download from a brightcove json item 
        
        if frame_width is specified, get the closest one  
        '''

        video_urls = [] # (width, encoding_rate, url)
        try:
            d_url  = b_json_item['FLVURL']
        except KeyError, e:
            _log.error("missing flvurl %s" % b_json_item)
            return None

        #If we get a broken response from brightcove api
        if not b_json_item.has_key('renditions'):
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
                                (abs(x[0]-self.platform.rendition_frame_width),
                                 -x[1]))
        else:
            #return the max width rendition with the highest encoding rate
            video_urls = sorted(video_urls, key=lambda x: (-x[0], -x[1]))
        return video_urls[0][2]

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
                    video_fields=BrightcoveIntegration.get_submit_video_fields(),
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

            cur_jobs = yield self.submit_many_videos(cur_results['videos'])
            retval.update(cur_jobs)

        raise tornado.gen.Return(retval)

    @tornado.gen.coroutine
    def submit_new_videos(self):
        '''Submits new videos in the account.'''
        from_date = datetime.datetime(1980, 1, 1)
        if (self.platform.last_process_date is not None and 
            not self.platform.uses_batch_provisioning):
            from_date = datetime.datetime.utcfromtimestamp(
                self.platform.last_process_date)

        video_iter = self.bc_api.find_modified_videos_iter(
            from_date=from_date,
            _filter=['UNSCHEDULED', 'INACTIVE', 'PLAYABLE'],
            sort_by='MODIFIED_DATE',
            sort_order='DESC',
            video_fields=BrightcoveIntegration.get_submit_video_fields(),
            custom_fields=self.get_custom_fields())

        count = 0
        last_mod_date = None

        while True:
            if (self.platform.last_process_date is None and 
                count >= options.max_vids_for_new_account):
                # New account, so only process the most recent videos
                break

            try:
                item = yield video_iter.next(async=True)
                if isinstance(item, StopIteration):
                    break
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

            if (self.platform.last_process_date is not None and 
                int(item['lastModifiedDate']) <=  
                (self.platform.last_process_date * 1000)):
                # No new videos
                break

            try: 
                yield self.submit_one_video_object(item, skip_old_video=True)
            except integrations.ovp.OVPCustomRefIDError: 
                pass 
            except integrations.ovp.OVPRefIDError: 
                pass 
            except Exception: 
                yield self.update_last_processed_date(last_mod_date)  

            count += 1
            last_mod_date = max(last_mod_date,
                                int(item['lastModifiedDate']))

        yield self.update_last_processed_date(last_mod_date) 

    @tornado.gen.coroutine
    def process_publisher_stream(self):
        yield self.submit_playlist_videos()
        yield self.submit_new_videos()

    @tornado.gen.coroutine 
    def update_last_processed_date(self, last_mod_date):
        if last_mod_date is not None:
            def _set_mod_date(x):
                x.last_process_date = last_mod_date / 1000.0
            self.platform = yield tornado.gen.Task(
                neondata.BrightcovePlatform.modify,
                self.platform.neon_api_key,
                self.platform.integration_id,
                _set_mod_date)
            
            _log.debug(
                'updated last process date for account %s integration %s'
                 % (self.platform.neon_api_key, self.platform.integration_id))

    @tornado.gen.coroutine
    def submit_many_videos(self, vid_objs, grab_new_thumb=True,
                           continue_on_error=False):
        '''Submits many video requests.

        Inputs:
        vid_objs - Iterator of video objects from Brightcove
        grab_new_thumbs - True if new thumbnails for the video should be
                          grabbed
        continue_on_error - If there is an error, do we continue and log
                            the exception in the return value.

        Returns: dictionary of video_id => job_id or exception
        
        '''
        retval = {}
        for data in vid_objs:
            try:
                retval[data['id']] = yield self.submit_one_video_object(
                    data, grab_new_thumb)
            except Exception as e:
                if continue_on_error:
                    retval[data['id']] = e
                else:                
                    raise

        raise tornado.gen.Return(retval)

    @tornado.gen.coroutine
    def submit_one_video_object(self, vid_obj, grab_new_thumb=True,
                                skip_old_video=False):
        '''Submits one video object

        Inputs:
        vid_obj - A video object from the response from the Brightcove API
        grab_new_thumb - True if new thumbnails for the video should be grabbed

        Outputs:
        the job id
        '''
        try:
            retval = yield self._submit_one_video_object_impl(vid_obj,
                                                              grab_new_thumb,
                                                              skip_old_video)
        except integrations.ovp.CMSAPIError as e:
            # Error is already logged
            raise
        except integrations.ovp.OVPError as e:
            raise
        except Exception as e:
            _log.exception('Unexpected error submiting video %s' % vid_obj)
            statemon.state.increment('unexpected_submition_error')
            raise
        raise tornado.gen.Return(retval)

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
                raise integrations.ovp.OVPRefIDError(msg)
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
                raise integrations.ovp.OVPCustomRefIDError(msg)
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

    @tornado.gen.coroutine
    def _update_video_info(self, data, bc_video_id, job_id):
        '''Update information in the database about the video.

        Inputs:
        data - A video object from Brightcove
        bc_video_id - The brightcove video id
        '''

        # Get the data that could be updated
        video_id = neondata.InternalVideoID.generate(
            self.platform.neon_api_key, bc_video_id)
        publish_date = data.get('publishedDate', None)
        if publish_date is not None:
            publish_date = datetime.datetime.utcfromtimestamp(
                int(publish_date) / 1000.0).isoformat()
        video_title = data.get('name', '')

        # Update the video object
        def _update_publish_date(x):
            x.publish_date = publish_date
            x.job_id = job_id
        video = yield tornado.gen.Task(
            neondata.VideoMetadata.modify,
            video_id,
            _update_publish_date)

        # Update the request object
        def _update_request(x):
            x.publish_date = publish_date
            x.video_title = video_title
        yield tornado.gen.Task(
            neondata.NeonApiRequest.modify,
            job_id, self.platform.neon_api_key, _update_request)
        
    @tornado.gen.coroutine
    def _grab_new_thumb(self, data, bc_video_id):
        '''Grab a new thumbnail from a video object if there is one.

        Inputs:
        data - A video object from Brightcove
        bc_video_id - The brightcove video id
        '''
        thumb_url, thumb_data = \
          BrightcoveIntegration._get_best_image_info(data)
        if thumb_data is None:
            _log.warn_n('Could not find thumbnail in %s' % data)
            return 
        bc_urls = [_normalize_thumbnail_url(x) for 
                   x in _get_urls_from_bc_response(data)]

        # Function that will set the external id in the ThumbnailMetadata
        external_id = thumb_data.get('id', None)
        if external_id is not None:
            external_id = unicode(external_id)
        def _set_external_id(obj):
            obj.external_id = external_id

        # Get the video object from our database
        video_id = neondata.InternalVideoID.generate(
            self.platform.neon_api_key, bc_video_id)
        vid_meta = yield tornado.gen.Task(neondata.VideoMetadata.get,
                                          video_id)
        if not vid_meta:
            _log.error('Could not find video %s' % video_id)
            statemon.state.increment('video_not_found')
            return

        # Search for the thumbnail already in our database
        thumbs = yield tornado.gen.Task(neondata.ThumbnailMetadata.get_many,
                                        vid_meta.thumbnail_ids)
        found_thumb = False
        min_rank = 1
        for thumb in thumbs:
            # Change the thumbnail type because the BRIGHTCOVE type is
            # deprecated
            if thumb.type == neondata.ThumbnailType.BRIGHTCOVE:
                def _set_type(obj):
                    obj.type = neondata.ThumbnailType.DEFAULT
                thumb = yield tornado.gen.Task(
                    neondata.ThumbnailMetadata.modify,
                    thumb.key,
                    _set_type)
            
            if (thumb is None or 
                thumb.type != neondata.ThumbnailType.DEFAULT):
                continue

            if thumb.rank < min_rank:
                min_rank = thumb.rank

            if thumb.external_id is not None:
                # We know about this thumb was in Brightcove so see if it
                # is still there.
                if (unicode(thumb.external_id) in 
                    _extract_image_info_from_bc_response(data, 'id')):
                    found_thumb = True
            elif thumb.refid is not None:
                # For legacy thumbs, we specified a reference id. Look for it
                if thumb.refid in _extract_image_info_from_bc_response(
                        data, 'referenceId'):
                    found_thumb = True

                    yield tornado.gen.Task(neondata.ThumbnailMetadata.modify,
                                           thumb.key,
                                           _set_external_id)
            else:
                # We do not have the id for this thumb, so see if we
                # can match the url.
                norm_urls = set([_normalize_thumbnail_url(x) 
                                 for x in thumb.urls])
                if len(norm_urls.intersection(bc_urls)) > 0:
                    found_thumb = True
                    
                    # Now update the external id because we didn't
                    # know about it before.
                    yield tornado.gen.Task(neondata.ThumbnailMetadata.modify,
                                           thumb.key,
                                           _set_external_id)

        is_exist = False
        if not found_thumb:
            # Add the thumbnail to our system
            urls = _get_urls_from_bc_response(data)
            added_image = False
            for url in urls[::-1]:
                try:
                    new_thumb = neondata.ThumbnailMetadata(
                        None,
                        ttype=neondata.ThumbnailType.DEFAULT,
                        rank = min_rank-1,
                        external_id = external_id
                        )

                    yield vid_meta.download_and_add_thumbnail(
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
                        any([new_thumb.key == old_thum.key
                            for old_thum in thumbs]) or \
                        any([new_thumb.phash == old_thum.phash
                            for old_thum in thumbs])

                    if is_exist:
                        continue

                    sucess = yield tornado.gen.Task(new_thumb.save)
                    if not sucess:
                        raise IOError("Could not save thumbnail")
                    # Even though the vid_meta already has the new_thumb
                    # in thumbnail_ids, the database still doesn't have it yet.
                    # We will modify in database first. Also, modify is used
                    # first instead of using save directively, as other process
                    # can modify the vid_meta as well.
                    updated_video = yield tornado.gen.Task(
                        vid_meta.modify,
                        vid_meta.key,
                        lambda x: x.thumbnail_ids.append(new_thumb.key))
                    if updated_video is None:
                        # It wasn't in the database, so save this object
                        sucess = yield tornado.gen.Task(vid_meta.save)
                        if not sucess:
                            raise IOError("Could not save video data")
                    else:
                        vid_meta.__dict__ = updated_video.__dict__

                    _log.info(
                        'Found new thumbnail %s for video %s at Brigthcove.' %
                        (external_id, vid_meta.key))
                    statemon.state.increment('new_images_found')
                    added_image = True
                    break
                except IOError:
                    # Error getting the image, so keep going
                    pass
            if not added_image and not is_exist:
                _log.error('Could not find valid image to add to video %s. '
                           'Tried urls %s' % (vid_meta.key, urls))
                statemon.state.increment('cant_get_image')
        
