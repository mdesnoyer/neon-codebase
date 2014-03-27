'''
An index that maps a url to a thumbnail.

Copyright: 2014 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from cv.imhash_index import ImHashIndex
import logging
import neondata
import threading
import tornado.gen
import tornado.httpclient
from utils.imageutils import PILImageUtils
import utils.sync

_log = logging.getLogger(__name__)

class URL2ThumbnailIndex:
    '''An index that converts from url to thumbnail metadata.

    If we don't know the mapping in the database, we try to find the
    image by its perceptual hash.

    This index can take a while to initialize because it must compute
    the perceptual hashes of all the images in the system.
        
    '''
    def __init__(self):
        '''Create the index. It is loaded from the neondata db.'''
        self.hash_index = ImHashIndex(hashtype='dhash', hash_size=64)
        self.phash_map = {} # pHash -> [thumbnail_id]

        self.thread_lock = threading.RLock()

    def build_index_from_neondata(self):
        '''Builds the index from the neondata database.'''
        _log.info('Collecting all the images in the database.')
        hashes = set()
        for thumb_info in neondata.ThumbnailMetadata.iterate_all_thumbnails():
            cur_hash = self._add_new_thumbnail_to_index(
                thumb_info,
                update_hash_index=False)
            if cur_hash is not None:
                hashes.add(cur_hash)

        _log.info('Building perceptual image index.')
        self.hash_index.build_index(hashes)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def add_thumbnail_to_index(self, thumbnail):
        '''Add an image to the index if it's not in there already.

        Inputs:
        thumbnail - A thumbnail metadata object.
        '''
        try:
            with self.thread_lock:
                if thumbnail.key in self.phash_map[thumbnail.phash]:
                    # This thumb is already in the index so we're done
                    raise tornado.gen.Return()
        except KeyError:
            pass

        yield tornado.gen.Task(self._add_new_thumbnail_to_index, thumbnail)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _add_new_thumbnail_to_index(self, thumb, update_hash_index=True):
        '''Adds the new thumbnail to the index.

        Returns its hash value
        '''
        # Go grab the thumbnail and compute its perceptual hash if
        # necessary
        image = None
        if thumb.phash is None:
            for url in thumb.urls:
                try:
                    image = yield tornado.gen.Task(
                        PILImageUtils.download_image, url)
                except Exception:
                    # Error logging happens in the download_image function
                    continue
                
                thumb.update_phash(image)
                    
                # Only need the hash from one url
                break

        if thumb.phash is not None:
            # Record the hash value in the index
            with self.thread_lock:
                try:
                    # Make sure that the thumb wasn't added in an async call
                    if (thumb.key not in self.phash_map[thumb.phash]):
                        self.phash_map[thumb.phash].append(thumb.key)
                except KeyError:
                    self.phash_map[thumb.phash] = [thumb.key]
                    if update_hash_index:
                        self.hash_index.add_hash(thumb.phash)

        if image is not None:
            # Update the phash in the thumbnail metadata
            def _setphash(x): x.phash = thumb.phash
            updated_thumb = yield tornado.gen.Task(
                neondata.ThumbnailMetadata.modify,
                thumb.key,
                _setphash)
            if updated_thumb is None:
                # This thumb is new, so save it
                yield tornado.gen.Task(thumb.save)

        raise tornado.gen.Return(thumb.phash)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_thumbnail_info(self, url, internal_video_id=None,
                           account_api_key=None):
        '''Retrieves the ThumbnailMetadata object for the given url.

        If we don't know the url, we fetch the image and try to match
        it to a known url with a perceptual hash. This is important
        because the image may have been resized, recompressed or
        copied to a different location along the way.

        If we are able to match an unknown url, it is recorded in the database.

        If no thumbnail information could be found, None is returned.

        Inputs:
        url - url of the image to lookup
        video_id - if set, the thumbnail must be from this video id
        account_api_key - if set, the thumbnail must be in an account with the 
                          given api key
        '''

        # First look for the URL in our mapper
        thumb_id = yield tornado.gen.Task(neondata.ThumbnailURLMapper.get_id,
                                          url)
        if thumb_id is not None:
            thumb_info = yield tornado.gen.Task(neondata.ThumbnailMetadata.get,
                                                thumb_id)
            if thumb_info is None:
                _log.error('Could not find thumbnail information for id: %s' %
                           thumb_id)
                raise tornado.gen.Return(None)
            elif (internal_video_id is not None and 
                  thumb_info.video_id != internal_video_id):
                _log.error("Asking for URL: %s with video id: %s, but the "
                           "thumbnail's video id is: %s. This should never "
                           "happen because URLs should be only for a single "
                           "video.", url, internal_video_id,
                           thumb_info.video_id)
                raise tornado.gen.Return(None)
            elif (account_api_key is not None and 
                  thumb_info.get_account_id() != account_api_key):
                _log.error("Asking for URL: %s with account id: %s, but the "
                           "thumbnail's account id is: %s. This should never "
                           "happen because URLs should be only for a single "
                           "video.", url, account_api_key,
                           thumb_info.get_account_id())
                raise tornado.gen.Return(None)
            raise tornado.gen.Return(thumb_info)

        # TODO(mdesnoyer): Once our image urls contain a thumbnail id,
        # we can try to extract that directly first.

        # We don't know about this url so get the image
        try:
            image = yield tornado.gen.Task(
                PILImageUtils.download_image, url)
        except Exception:
            # Error logging happens in the download_image function
            raise tornado.gen.Return(None)
        phash = self.hash_index.hash_pil_image(image)
        
        # Lookup similar images in the hash index
        possible_thumbs = []
        with self.thread_lock:
            for cur_hash, dist in self.hash_index.radius_search(phash):
                try:
                    for thumb_id in self.phash_map[cur_hash]:
                        possible_thumbs.append((dist, thumb_id))
                except KeyError:
                    pass

        if len(possible_thumbs) == 0:
            raise tornado.gen.Return(None)

        # Sort the possible thumbs by distance
        possible_thumbs = sorted(possible_thumbs)
        
        # Filter out the thumbs not in this account and/or video id
        dists, thumb_ids = zip(*possible_thumbs)
        thumbs = yield tornado.gen.Task(neondata.ThumbnailMetadata.get_many,
                                        thumb_ids)
        possible_thumbs = filter(
            lambda x: (account_api_key is None or 
                       account_api_key == x[1].get_account_id()) and 
                      (internal_video_id is None or 
                       internal_video_id == x[1].video_id),
            zip(dists, thumbs))
        
        
        if (len(possible_thumbs) == 1 or 
            (len(possible_thumbs) > 1 and 
             possible_thumbs[0][0] < possible_thumbs[1][0])):
            # We found a unique enough match for the url, so record it
            found_thumb = possible_thumbs[0][1]

            # Atomically update the thumbnail information. Technically,
            # we don't need to wait for the save to happen, but it
            # simlifies testing, so do it.
            def _update_thumb(thumb):
                thumb.urls.append(url)
                if possible_thumbs[0][0] == 0:
                    thumb.phash = phash
            updated_thumb = yield tornado.gen.Task(
                neondata.ThumbnailMetadata.modify,
                found_thumb.key,
                _update_thumb)

            # Add an entry for this url in the URL mapper
            yield tornado.gen.Task(
                neondata.ThumbnailURLMapper(url, updated_thumb.key).save)

            raise tornado.gen.Return(updated_thumb)

        # No match was found
        raise tornado.gen.Return(None)
