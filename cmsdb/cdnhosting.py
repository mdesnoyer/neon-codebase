'''
Host images on the CDN Module
'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import api.akamai_api
import base64
import boto.exception
import cmsdb.neondata
import imageio
import json
import hashlib
import random
import re
import socket
import string
import tempfile
import time
import tornado.gen
import urllib
import urllib2
import urlparse
import utils.s3

from boto.exception import S3ResponseError, BotoServerError, BotoClientError
from boto.s3.connection import S3Connection
from poster.encode import multipart_encode
from StringIO import StringIO
import utils.botoutils
from cvutils.imageutils import PILImageUtils
from utils import pycvutils
from utils import statemon
import utils.sync
from cvutils import smartcrop

import logging
_log = logging.getLogger(__name__)

from utils.options import define, options
define('hosting_bucket', default='host-thumbnails',
       help='Bucket that will host images in S3')
define('cloudinary_name', default='neon-labs',
       help='Account name in cloudinary')
define('cloudinary_api_key', default='433154993476843',
       help='Cloudinary api key')
define('cloudinary_api_secret', default='n0E7427lrS1Fe_9HLbtykf9CdtA',
       help='Cloudinary secret api key')

# Monitoring
statemon.define('upload_error', int)
statemon.define('s3_upload_error', int)
statemon.define('akamai_upload_error', int)
statemon.define('invalid_cdn_url', int)

def get_s3_hosting_bucket():
    '''Returns the bucket that hosts the images.'''
    return options.hosting_bucket

@utils.sync.optional_sync
@tornado.gen.coroutine
def create_s3_redirect(dest_key, src_key, dest_bucket=None,
                       src_bucket=None, content_type=None):
    '''Creates a 301 redirect in s3 that points to another location.

    Inputs:
    @dest_key: The key of the redirect to point to
    @src_key: The key that will hold the redirect
    @src_bucket: S3 bucket where the redirect object will be.
                 Defaults to the image hosting bucket
    @dest_bucket: S3 bucket where the redirect object will be.
                  Defaults to the image hosting bucket
    '''
    if src_bucket is None:
        src_bucket = get_s3_hosting_bucket()
    if dest_bucket is None:
        dest_bucket = get_s3_hosting_bucket()


    if src_bucket == dest_bucket:
        redirect_loc = "/%s" % dest_key
    else:
        redirect_loc = "https://s3.amazonaws.com/%s/%s" % (
            dest_bucket, dest_key)

    headers = {'x-amz-website-redirect-location' : redirect_loc}
    if content_type is not None:
        headers['Content-Type'] = content_type

    s3conn = S3Connection()
    try:
        bucket = yield utils.botoutils.run_async(s3conn.get_bucket, src_bucket)
        key = bucket.new_key(src_key)
        yield utils.botoutils.run_async(
            key.set_contents_from_string,
            '',
            headers=headers,
            policy='public-read')
    except BotoServerError as e:
        _log.error('AWS Server error when creating a redirect s3://%s/%s -> '
                   '%s : %s' % (src_bucket, src_key, redirect_loc, e))
        raise IOError(str(e))

    except BotoClientError as e:
        _log.error('AWS client error when creating a redirect s3://%s/%s -> '
                   '%s : %s' %  (src_bucket, src_key, redirect_loc, e))
        raise IOError(str(e))


class CDNHosting(object):
    '''Abstract class for hosting images on a CDN.'''
    def __init__(self, cdn_metadata):
        '''Abstract CDN hosting class.

        @cdn_metadata - The metadata specifying how to access the CDN
        @accepts_images - If true, upload functions takes images.
                          otherwise they take urls.
        '''
        self.resize = cdn_metadata.resize
        self.update_serving_urls = cdn_metadata.update_serving_urls
        self.rendition_sizes = cdn_metadata.rendition_sizes or []
        self.video_rendition_sizes = cdn_metadata.video_rendition_sizes or []
        self.cdn_prefixes = cdn_metadata.cdn_prefixes
        self.source_crop = cdn_metadata.source_crop
        self.crop_with_saliency = cdn_metadata.crop_with_saliency
        self.crop_with_face_detection = cdn_metadata.crop_with_face_detection
        self.crop_with_text_detection = cdn_metadata.crop_with_text_detection

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def upload(self,
               image,
               tid,
               url=None,
               overwrite=True,
               servingurl_overwrite=False,
               do_source_crop=False,
               do_smart_crop=False):
        '''
        Host images on the CDN

        The sizes for a given image is specified in the rendition_sizes
        size is a tuple (width, height)

        Saves the mappings in ThumbnailServingURLs object

        Inputs:
        image - Image to upload
        tid - Thumbnail id of the image
        url - URL of the image that's already live. This is optional but some
              CDNHosting objects might use this instead of the image.
        overwrite - Should existing files be overwritten?
        servingurl_overwrite - Should the serving urls be overwritten?
        do_source_crop - Will source crop, so long as self.source_crop is
                         defined. See notes on source cropping below.
        do_smart_crop - Will smart crop images.

        Source Crop:
            source cropping excludes regions of the images that are known to
            be bad, i.e., the newscrawl with CNN images.

        Returns: list [(cdn_url, width, height)]
        '''
        # we need to avoid source cropping (and smart cropping) thumbnails
        # that come directly from the client.
        new_serving_thumbs = [] # (url, width, height)

        if self.source_crop is not None and do_source_crop:
            if not self.resize:
                _log.error(('Crop source specified but no desired final ',
                            'size is defined'))
                raise ValueError(('Crop source can only operate along with ',
                                  'resize'))
            _prep = pycvutils.ImagePrep(crop_frac=self.source_crop,
                                        return_same=True)
            image = _prep(image)
        # NOTE: if _upload_impl returns None, the image is not added to the
        # list of serving URLs
        try:
            if self.resize:
                cv_im = pycvutils.from_pil(image)
                if do_smart_crop:
                    sc = smartcrop.SmartCrop(cv_im,
                        with_saliency=self.crop_with_saliency,
                        with_face_detection=self.crop_with_face_detection,
                        with_text_detection=self.crop_with_text_detection)
                for sz in self.rendition_sizes:
                    width = sz[0]
                    height = sz[1]
                    cv_im = pycvutils.from_pil(image)
                    if do_smart_crop:
                        cv_im_r = sc.crop_and_resize(height, width)
                    else:
                        # avoid smart cropping, since it's too aggressive
                        # about finding text.
                        cv_im_r = pycvutils.resize_and_crop(
                            cv_im, height, width)
                    im = pycvutils.to_pil(cv_im_r)

                    image_file = StringIO()
                    im.save(image_file, 'jpeg', quality=90)
                    image_file.seek(0)

                    cdn_val = yield self._upload_and_check_file(
                        image_file, 'jpeg', tid, width, height, url, overwrite)
                    new_serving_thumbs.append(cdn_val)
            else:
                image_file = StringIO()
                image.save(image_file, 'jpeg', quality=90)
                image_file.seek(0)
                width = image.size[0]
                height = image.size[1]

                cdn_val = yield self._upload_and_check_file(
                    image_file, 'jpeg', tid, width, height, url, overwrite)
                new_serving_thumbs.append(cdn_val)

        except IOError:
            statemon.state.increment('upload_error')
            raise

        if self.update_serving_urls and len(new_serving_thumbs) > 0:
            def add_serving_urls(obj):
                for params in new_serving_thumbs:
                    obj.add_serving_url(*params)

            if servingurl_overwrite:
                url_obj = cmsdb.neondata.ThumbnailServingURLs(tid)
                add_serving_urls(url_obj)
                url_obj.save()
            else:
                yield tornado.gen.Task(
                    cmsdb.neondata.ThumbnailServingURLs.modify,
                    tid,
                    add_serving_urls,
                    create_missing=True)

        # return the CDN URL
        raise tornado.gen.Return(new_serving_thumbs)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def upload_video(self, video, clip_id, start, end, url=None):
        '''Render renditions. Put them to CDN

        Inputs- video a cv2.VideoCapture
            -clip_id id of the containing Clip
            -start integer, start frame
            -end integer, end frame
            -url the url that the file should be put to or None
                if not provided, then we will build one from ids

        # Results is a list of [url, width, height, container, codec]s.
        '''

        # List of 3-tuple (url, width, height, container, codec) for
        # each new object.
        results = []

        for size in self.video_rendition_sizes:

            # Build a ffmpeg param to scale to the specified size.
            width = size[0] or cv2.get(CAP_PROP_FRAME_WIDTH)
            height = size[1] or cv2.get(CAP_PROP_FRAME_HEIGHT)
            ffmpeg_params = []
            if size[0] and size[1]:
                ffmpeg_params = ['-vf', 'scale=%s:%s' % (width, height)]

            # Use the specified video container type.
            container_type = size[2]
            if container_type == cmsdb.neondata.VideoRenditionContainerType.MP4:
                suffix = '.mp4'
            else:
                raise ValueError('Unhandled video container type %s', 
                                 container_type)

            # Get a writer with a named temporary file with the
            # right file extension.
            with tempfile.NamedTemporaryFile(suffix=suffix) as target:
                with imageio.get_writer(target.name, 'FFMPEG', fps=29.97, ffmpeg_params=ffmpeg_params) as writer:

                    try:
                        for frame in pycvutils.iterate_video(video, start, end):
                            writer.append_data(frame[:,:,::-1])
                        cdn_val = yield self._upload_and_check_file(
                            target, 'mp4', clip_id, width, height, url, False)
                        if cdn_val:
                            # Include container and codec from size.
                            results.append(cdn_val + size[2:2])
                    except Exception as e:
                        _log.error('Failed to generate or upload video %s', e)

        # Return the new object urls.
        raise tornado.gen.Return(results)

    @tornado.gen.coroutine
    def _upload_and_check_file(self, _file, _format, key, width, height,
                               url, overwrite):
        '''Returns a tuple of (cdn_url, width, height).'''

        cdn_url = yield self._upload_impl(
            _file,
            _format,
            key,
            width,
            height,
            url,
            overwrite,
            async=True)

        is_cdn_url_valid = yield self._check_cdn_url(cdn_url)
        if not is_cdn_url_valid:
            msg = 'CDN url %s is invalid' % cdn_url
            _log.error_n(msg, 30);
            statemon.state.increment('invalid_cdn_url')
            raise IOError(msg)
        elif cdn_url is not None:
            raise tornado.gen.Return((cdn_url, width, height))
        raise tornado.gen.Return(None)

    @tornado.gen.coroutine
    def _check_cdn_url(self, url):
        '''Returns True if we have a valid response from the CDN URL.'''
        request = tornado.httpclient.HTTPRequest(
            url, 'GET',
            headers={'Accept': 'image/*'})
        response = yield utils.http.send_request(
            request,
            base_delay=10.0,
            async=True)
        if response.error:
            raise tornado.gen.Return(False)
        raise tornado.gen.Return(True)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _upload_impl(self, _file, _format, key, width, height, url=None, overwrite=True):
        '''Upload the specific image to the CDN service.

        Note that this could be called multiple times for the same
        image, but they could be different sizes.

        Your implementation must create urls with the same base (all
        the way up to the last '/') for each thumbnail id. If you use
        a random number, then, the easiest thing to do is to use
        rng = random.Random(key)
        # Figure out the cdn url using rng

        To be implemented by a subclass.

        Inputs:
        @image: PIL image to upload
        @key: id of the metadata object of this file
        @url: URL of the image that's already live. This is optional but some
              CDNHosting objects might use this instead of the image.
        @overwrite: If True, an existing file on the CDN will be overwritten

        @returns: The serving url for the image
        @raises: IOError if the image couldn't be uploaded
        '''
        raise NotImplementedError()

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete(self, url):
        '''Delete the object at a given url.

        Inputs:
        @url: The url to delete from the CDN
        '''
        raise NotImplementedError()

    @staticmethod
    def create(cdn_metadata):
        '''
        Creates the appropriate connection based on a database entry.
        '''
        if isinstance(cdn_metadata,
                      cmsdb.neondata.S3CDNHostingMetadata):
            return AWSHosting(cdn_metadata)
        elif isinstance(cdn_metadata,
                        cmsdb.neondata.CloudinaryCDNHostingMetadata):
            return CloudinaryHosting(cdn_metadata)
        elif isinstance(cdn_metadata,
                        cmsdb.neondata.AkamaiCDNHostingMetadata):
            return AkamaiHosting(cdn_metadata)

        else:
            raise ValueError("CDNHosting type %s not supported yet, please"
                             " implement" % cdn_metadata.__class__.__name__)

class AWSHosting(CDNHosting):

    def __init__(self, cdn_metadata):
        super(AWSHosting, self).__init__(cdn_metadata)
        self.policy = cdn_metadata.policy
        if self.policy is None:
            neon_bucket = (isinstance(
                cdn_metadata, cmsdb.neondata.NeonCDNHostingMetadata)
                or isinstance(
                    cdn_metadata,
                    cmsdb.neondata.PrimaryNeonHostingMetadata))
            if neon_bucket:
                self.policy = 'public-read'
        self.s3conn = S3Connection(cdn_metadata.access_key,
                                   cdn_metadata.secret_key)
        self.s3bucket_name = cdn_metadata.bucket_name
        self.s3bucket = None
        self.cdn_prefixes = cdn_metadata.cdn_prefixes
        if cdn_metadata.folder_prefix:
            self.folder_prefix = cdn_metadata.folder_prefix.strip('/')
        else:
            self.folder_prefix = None
        self.do_salt = cdn_metadata.do_salt
        self.make_tid_folders = cdn_metadata.make_tid_folders

    @tornado.gen.coroutine
    def _get_bucket(self):
        '''Connects to the bucket if it's not already done'''
        if self.s3bucket is None:
            try:
                self.s3bucket = yield utils.botoutils.run_async(
                    self.s3conn.get_bucket,
                    self.s3bucket_name)
            except S3ResponseError as e:
                if e.status == 403:
                    # It's a permissions error so just get the bucket
                    # and don't validate it
                    self.s3bucket = self.s3conn.get_bucket(
                        self.s3bucket_name, validate=False)
                else:
                    raise
        raise tornado.gen.Return(self.s3bucket)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _upload_impl(self, _file, _format, key, width, height, url=None,
                     overwrite=True):

        rng = random.Random(key)

        if _format not in ['mp4', 'jpeg']:
            raise ValueError('Unknown file upload format %s' % _format)

        s3bucket = yield self._get_bucket()

        if self.cdn_prefixes and len(self.cdn_prefixes) > 0:
            cdn_prefix = rng.choice(self.cdn_prefixes)
        else:
            cdn_prefix = "http://s3.amazonaws.com/%s" % self.s3bucket_name

        # Build the key name
        name_pieces = []
        if self.folder_prefix:
            name_pieces.append(self.folder_prefix)
        if self.do_salt:
            name_pieces.append(''.join(
                rng.choice(string.letters + string.digits)
                for _ in range(3)))
        if self.make_tid_folders:
            name_pieces.append("%s.jpg" % re.sub('_', '/', key))
        else:
            name_pieces.append(
                cmsdb.neondata.ThumbnailServingURLs.create_filename(
                key, width, height))
        key_name = '/'.join(name_pieces)

        cdn_url = "%s/%s" % (cdn_prefix, key_name)

        try:
            try:
                key = s3bucket.get_key(key_name)
            except S3ResponseError as e:
                if e.status == 403:
                    key = None
                else:
                    raise
            if key is None:
                key = s3bucket.new_key(key_name)
            elif not overwrite:
                # We're done because the object is already there
                raise tornado.gen.Return(cdn_url)
            else:
                # We are overwriting, but check the size to see if it
                # matches. If it does, don't bother uploading because
                # it's probably the same image. Thank you lossy jpeg
                # compression. I'd love to do an md5, but we don't get
                # that from S3
                if key.size and _file.len == key.size:
                    raise tornado.gen.Return(cdn_url)

            if _format == 'mp4':
                content_type = {'Content-Type': 'video/mp4'}
            else:
                content_type = {'Content-Type': 'image/jpeg'}

            yield utils.botoutils.run_async(
                key.set_contents_from_file,
                _file,
                content_type,
                policy=self.policy,
                replace=overwrite)

        except BotoServerError as e:
            _log.error_n(
            'AWS Server error when uploading file to s3://%s/%s: %s' %
                    (self.s3bucket_name, key_name, e))
            statemon.state.increment('s3_upload_error')
            raise IOError(str(e))

        except BotoClientError as e:
            _log.error_n(
                'AWS client error when uploading file to s3://%s/%s : %s' %
                    (self.s3bucket_name, key_name, e))
            statemon.state.increment('s3_upload_error')

            raise IOError(str(e))

        raise tornado.gen.Return(cdn_url)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete(self, url):
        '''Delete the object at a given url.

        Inputs:
        @url: The url to delete from the CDN
        '''
        s3bucket = yield self._get_bucket()

        key_name = urlparse.urlparse(url).path.strip('/')
        try:
            yield utils.botoutils.run_async(s3bucket.delete_key,
                                            key_name)
        except boto.exception.StorageResponseError as e:
            # key wasn't there, so that's ok
            pass

class CloudinaryHosting(CDNHosting):

    '''
    Upload a single sized base image to Cloudinary

    uploaded images look like the following
    http://res.cloudinary.com/neon-labs/image/upload/{CloudinaryUID}/{NEON_TID}.jpg

    To dynamically resize these images, the cloudinary UID is to be replaced by
    the requeired dimensions as in this example w_120,h_90
    http://res.cloudinary.com/neon-labs/image/upload/w_120,h_90/{NEON_TID}.jpg

    '''
    def __init__(self, cdn_metadata):
        super(CloudinaryHosting, self).__init__(cdn_metadata)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _upload_impl(self, _file, _format, key, width, height, url=None,
                     overwrite=True):
        '''
        Upload the image to cloudinary.

        Note: No support for uploading raw images yet
        '''

        if url is None:
            raise ValueError("Cloudinary hosting must be done by specifying "
                             "the url")

        # 0, 0 indicates original (base image size)
        img_name = "neontn%s_w%s_h%s.jpg" % (key, "0", "0")

        params = {}
        params['timestamp'] = int(time.time())
        params['public_id'] = img_name
        #params['use_filename'] = True #original file name of the uploaded image
        #params['unique_filename'] = False #don't add random characters at the end of the filename

        self.sign_request(params)
        headers = {}

        params['file'] = url
        response = yield self.make_request(params, None, headers, async=True)
        if response.error:
            msg = ("Failed to upload file to cloudinary for key %s: %s" %
                   (key, response.error))
            _log.error_n(msg)
            raise IOError(msg)

        # TODO: This upload function doesn't conform, fix if we want
        # to use cloudinary again.
        raise tornado.gen.Return(None)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def make_request(self, params, imdata=None, headers=None):
        api_url  = "https://api.cloudinary.com/v1_1/%s/image/upload" %\
                     options.cloudinary_name

        encoded_params = urllib.urlencode(params)
        request = tornado.httpclient.HTTPRequest(api_url, 'POST',
                                                 body=encoded_params)

        try:
            response = yield utils.http.send_request(request, async=True)
            raise tornado.gen.Return(response)
        except socket.error, e:
            _log.error("Socket error uploading image to cloudinary %s" %\
                        params['public_id'])
            raise IOError('Error connecting to cloudinary')
        except tornado.httpclient.HTTPError, e:
            _log.error("http error uploading image to cloudinary %s" %\
                        params['public_id'])
            raise IOError('Error uploading to cloudinary')

    def sign_request(self, params):

        api_secret = options.cloudinary_api_secret

        params["signature"] = self.api_sign_request(params, api_secret)
        params["api_key"] = options.cloudinary_api_key

        return params

    def api_sign_request(self, params_to_sign, api_secret):
        '''
        You need to sign a string with all parameters sorted by their names alphabetically.
        Separate parameter name and value with '=' and join parameters with '&'.
        '''
        to_sign = "&".join(sorted([(k+"="+(",".join(v) if isinstance(v, list) else str(v))) for k, v in params_to_sign.items() if v]))
        return hashlib.sha1(str(to_sign + api_secret)).hexdigest()



class AkamaiHosting(CDNHosting):

    def __init__(self, cdn_metadata):
        super(AkamaiHosting, self).__init__(cdn_metadata)
        if cdn_metadata.folder_prefix:
            self.folder_prefix = cdn_metadata.folder_prefix.strip('/')
        else:
            self.folder_prefix = None
        self.ak_conn = api.akamai_api.AkamaiNetstorage(
            cdn_metadata.host,
            cdn_metadata.akamai_key,
            cdn_metadata.akamai_name,
            cdn_metadata.cpcode)
        self.ntries = 5

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _upload_impl(self, _file, _format, key, width, height, url=None,
            overwrite=True):
        rng = random.Random(key)

        cdn_prefix = rng.choice(self.cdn_prefixes)

        # Akamai storage does not recommend a single folder for all
        # files under our neon account for performance reasons (limit 2000).
        # Therefore we need a folder structure, one where we can spread
        # the pictures with a reasonable expectation of staying within
        # the limit.
        #
        # We use here a 4 folders deep structure. The root folder is the
        # customer account id.  It is followed by 3 sub folders with single
        # randomly selected letters. This structure affords over 281 million
        # elements before reaching the recommended limit for a given account

        # the customer account root folder id is taken from the key. This may
        # break in the future if the key scheme changes. Another option would
        # be to add a root folder to the class that would be set using the
        # account id. For now, this is fine so go with it.
        name_pieces = []
        if self.folder_prefix:
            name_pieces.extend(self.folder_prefix.split('/'))

        name_pieces.append(key[:24])
        for _ in range(3):
            name_pieces.append(rng.choice(string.ascii_letters))

        # Add the filename
        name_pieces.append(
            cmsdb.neondata.ThumbnailServingURLs.create_filename(
                key, width, height))

        path = '/'.join(name_pieces)

        # the full cdn url
        cdn_url = "%s/%s" % (cdn_prefix, path)

        # If we do not overwrite and it's already there, stop
        if not overwrite:
            stat_response = yield self.ak_conn.stat(path, ntries=1,
                                                    do_logging=False,
                                                    async=True)
            if stat_response.code == 200:
                raise tornado.gen.Return(cdn_url)

        response = yield self.ak_conn.upload(path, _file.read(),
                                             ntries=self.ntries,
                                             async=True)
        if response.error:
            msg = ("Error uploading file to akamai for key %s: %s"
                   % (key, response.error))
            _log.error_n(msg)
            statemon.state.increment('akamai_upload_error')
            raise IOError(str(msg))

        raise tornado.gen.Return(cdn_url)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def delete(self, url):
        '''Delete the object at a given url.

        Inputs:
        @url: The url to delete from the CDN
        '''

        rel_path = urlparse.urlparse(url).path.strip('/')
        response = yield self.ak_conn.delete(rel_path, async=True)
        if response.error and response.error.code != 404:
            msg = ("Error delete file %s from akamai: %s"
                   % (url, response.error))
            _log.error_n(msg)
            raise IOError(str(msg))
