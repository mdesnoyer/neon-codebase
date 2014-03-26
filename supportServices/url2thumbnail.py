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
import PIL.Image
import threading
import tornado.httpclient
import tornado.gen
import utils.http
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
        self.phash_map = {} # pHash -> [thumbnail_mapper_obj]

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
                if thumbnail.key in [x.key for x in 
                                     self.phash_map[thumbnail.phash]]:
                    # This thumb is already in the index so we're done
                    return
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
        if thumb.phash is None:
            for url in thumb.urls:
                response = yield tornado.gen.Task(utils.http.send_request,
                    tornado.httpclient.HTTPRequest(url))
                if response.error:
                    _log.error('Error retrieving image from: %s. %s' % 
                               (url, response.error))
                    continue

                image = PIL.Image.open(response.buffer)
                thumb.update_phash(image)
                    
                # Only need the hash from one url
                break

        if thumb.phash is not None:
            # Record the hash value in the index
            with self.thread_lock:
                try:
                    self.phash_map[thumb.phash].append(thumb)
                except KeyError:
                    self.phash_map[thumb.phash] = [thumb]
                    if update_hash_index:
                        self.hash_index.add_hash(thumb.phash)

        
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
            raise tornado.gen.Return(thumb_info)

        # TODO(mdesnoyer): Once our image urls contain a thumbnail id,
        # we can try to extract that directly first.

        # We don't know about this url so get the image
        response = yield tornado.gen.Task(utils.http.send_request,
            tornado.httpclient.HTTPRequest(url))
        if response.error:
            _log.error('Error retrieving image from: %s: %s' %
                       (url, response.error))
            raise tornado.gen.Return(None)
        image = PIL.Image.open(response.buffer)
        phash = self.hash_index.hash_pil_image(image)
        
        # Lookup similar images in the hash index
        possible_thumbs = []
        for cur_hash, dist in self.hash_index.radius_search(phash):
            try:
                for thumb_info in self.phash_map[cur_hash]:
                    if internal_video_id is not None:
                        if thumb_info.video_id != internal_video_id:
                            continue

                    if account_api_key is not None:
                        if account_api_key != thumb_info.get_account_id():
                            continue
                    possible_thumbs.append((dist, thumb_info))
            except KeyError:
                pass
            
        possible_thumbs = sorted(possible_thumbs)
        if (len(possible_thumbs) == 1 or 
            (len(possible_thumbs) > 1 and 
             possible_thumbs[0][0] < possible_thumbs[1][0])):
            # We found a unique enough match for the url, so record it
            found_thumb = possible_thumbs[0][1]
            found_thumb.urls.append(url)

            # Record the phash so it's not calculated later
            if possible_thumbs[0][0] == 0:
                found_thumb.phash = phash

            # We don't need to wait for the save to happen here, but
            # it simplifies testing, so do it.
            yield tornado.gen.Task(found_thumb.save)

            raise tornado.gen.Return(found_thumb)

        # No match was found
        raise tornado.gen.Return(None)
