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
import json
import hashlib
import random
import re
import socket
import string
import cmsdb.neondata
import time
import tornado.gen
import urllib
import urllib2
import utils.s3

from boto.exception import S3ResponseError, BotoServerError, BotoClientError
from boto.s3.connection import S3Connection
from poster.encode import multipart_encode
from StringIO import StringIO
import utils.botoutils
from utils.imageutils import PILImageUtils
from utils import pycvutils
import utils.sync

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

@utils.sync.optional_sync
@tornado.gen.coroutine
def upload_image_to_s3(
        keyname,
        data,
        bucket=None,
        bucket_name=options.hosting_bucket,
        access_key=None,
        secret_key=None,
        *args, **kwargs):
    '''
    Upload image to s3 bucket 'host-thumbnails' 

    This is the bucket where the primary copy of thumbnails are stored.
    ThumbnailMetadata structure stores these thumbnails
    CMS API returns these urls to be populated in the Neon UI

    @keyname: the basename of the file or name relative to bucket 
    @data: string data (of image) to be uploaded to S3 
    @bucket: Optional bucket object to upload to.
             Allows reuse of the bucket object.
    @bucket_name: bucket to upload to
    @access_key: s3 key
    @secret_key: s3 secret key
    *args, **kwargs: Arguments passed to set_contents_from_string 
    '''
    try:
        if bucket is None:
            s3conn = S3Connection(access_key, secret_key)
            bucket = yield utils.botoutils.run_async(s3conn.get_bucket,
                                                     bucket_name)
        else:
            bucket_name = bucket.name

        key = bucket.new_key(keyname)

        yield utils.botoutils.run_async(
            key.set_contents_from_string,
            data,
            {"Content-Type":"image/jpeg"},
            *args, **kwargs)
    except BotoServerError as e:
        _log.error(
            'AWS Server error when uploading image to s3://%s/%s: %s' % 
                    (bucket_name, keyname, e))
        raise IOError(str(e))

    except BotoClientError as e:
        _log.error(
            'AWS client error when uploading image to s3://%s/%s : %s' % 
                    (bucket_name, keyname, e))
        raise IOError(str(e))

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

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def upload(self, image, tid, url=None):
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
        '''
        new_serving_thumbs = [] # (url, width, height)
        
        # NOTE: if _upload_impl returns None, the image is not added to the 
        # list of serving URLs

        if self.resize:
            for sz in self.rendition_sizes:
                cv_im = pycvutils.from_pil(image)
                cv_im_r = pycvutils.resize_and_crop(cv_im, sz[1], sz[0])
                im = pycvutils.to_pil(cv_im_r)
                cdn_url = yield self._upload_impl(im, tid, url,
                                                  async=True)
                if cdn_url:
                    new_serving_thumbs.append((cdn_url, sz[0], sz[1]))

        else:
            cdn_url = yield self._upload_impl(image, tid, url,
                                              async=True)
            # Append only if the image was uploaded successfully   
            if cdn_url:
                new_serving_thumbs.append((cdn_url, image.size[0], image.size[1]))

        if self.update_serving_urls:
            def add_serving_urls(obj):
                for params in new_serving_thumbs:
                    obj.add_serving_url(*params)

            yield tornado.gen.Task(
                cmsdb.neondata.ThumbnailServingURLs.modify,
                tid,
                add_serving_urls,
                create_missing=True)
        
        # return the CDN URL 
        raise tornado.gen.Return(cdn_url)
        
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _upload_impl(self, image, tid, url=None):
        '''Upload the specific image to the CDN service.

        Note that this could be called multiple times for the same
        image, but they could be different sizes.

        Your implementation must create urls with the same base (all
        the way up to the last '/') for each thumbnail id. If you use
        a random number, then, the easiest thing to do is to use
        rng = random.Random(tid)
        # Figure out the cdn url using rng

        To be implemented by a subclass.

        Inputs:
        @image: PIL image to upload
        @tid: tid of the image being uploaded
        @url: URL of the image that's already live. This is optional but some
              CDNHosting objects might use this instead of the image.

        @returns: The serving url for the image or None if it shouldn't be served
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

    neon_fname_fmt = "neontn%s_w%s_h%s.jpg" 
    
    def __init__(self, cdn_metadata):
        super(AWSHosting, self).__init__(cdn_metadata)
        self.neon_bucket = (isinstance(
            cdn_metadata, cmsdb.neondata.NeonCDNHostingMetadata)
            or isinstance(
                cdn_metadata,
                cmsdb.neondata.PrimaryNeonHostingMetadata))
        self.s3conn = S3Connection(cdn_metadata.access_key,
                                   cdn_metadata.secret_key)
        self.s3bucket_name = cdn_metadata.bucket_name
        self.s3bucket = None
        self.cdn_prefixes = cdn_metadata.cdn_prefixes
        self.folder_prefix = cdn_metadata.folder_prefix or ''
        if self.folder_prefix.endswith('/'):
            self.folder_prefix = self.folder_prefix[:-1]
        self.do_salt = cdn_metadata.do_salt
        self.make_tid_folders = cdn_metadata.make_tid_folders

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _upload_impl(self, image, tid, url=None):
        rng = random.Random(tid)
        
        # Connect to the bucket if not done already
        if self.s3bucket is None:
            self.s3bucket = yield utils.botoutils.run_async(
                self.s3conn.get_bucket,
                self.s3bucket_name)
        
        if self.cdn_prefixes and len(self.cdn_prefixes) > 0:
            cdn_prefix = rng.choice(self.cdn_prefixes)
        else:
            cdn_prefix = "s3.amazonaws.com/%s" % self.s3bucket_name


        # Build the key name
        name_pieces = []
        if self.folder_prefix:
            name_pieces.append(self.folder_prefix)
        if self.do_salt:
            name_pieces.append(''.join(
                rng.choice(string.letters + string.digits) 
                for _ in range(3)))
        if self.make_tid_folders:
            name_pieces.append("%s.jpg" % re.sub('_', '/', tid))
        else:
            name_pieces.append(AWSHosting.neon_fname_fmt % 
                               (tid, image.size[0], image.size[1]))
        key_name = '/'.join(name_pieces)

        cdn_url = "http://%s/%s" % (cdn_prefix, key_name)
        fmt = 'jpeg'
        filestream = StringIO()
        image.save(filestream, fmt, quality=90) 
        filestream.seek(0)
        imgdata = filestream.read()

        # You may not have permission to do this for
        # customer bucket, so check if neon bucket 
        policy = None
        if self.neon_bucket:
            policy = 'public-read'

        yield upload_image_to_s3(key_name, imgdata, bucket=self.s3bucket,
                                 policy=policy, async=True)

        raise tornado.gen.Return(cdn_url)

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
    def _upload_impl(self, image, tid, url=None):
        '''
        Upload the image to cloudinary.
        
        Note: No support for uploading raw images yet 
        '''

        if url is None:
            raise ValueError("Cloudinary hosting must be done by specifying "
                             "the url")

        # 0, 0 indicates original (base image size)
        img_name = "neontn%s_w%s_h%s.jpg" % (tid, "0", "0") 
        
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
            _log.error("Failed to upload image to cloudinary for tid %s: %s" %
                       (tid, response.error))

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def make_request(self, params, imdata=None, headers=None):
        api_url  = "https://api.cloudinary.com/v1_1/%s/image/upload" %\
                     options.cloudinary_name          
        
        encoded_params = urllib.urlencode(params)
        request = tornado.httpclient.HTTPRequest(api_url, 'POST',
                                                 body=encoded_params)
        
        try:
            response = yield tornado.gen.Task(utils.http.send_request, request) 
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

    neon_fname_fmt = "neontn%s_w%s_h%s.jpg" 

    def __init__(self, cdn_metadata):
        super(AkamaiHosting, self).__init__(cdn_metadata)
        self.cdn_prefixes = cdn_metadata.cdn_prefixes 
        self.ak_conn = api.akamai_api.AkamaiNetstorage(
            cdn_metadata.host,
            cdn_metadata.akamai_key,
            cdn_metadata.akamai_name,
            cdn_metadata.baseurl)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def _upload_impl(self, image, tid, url=None):
        rng = random.Random(tid)

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
        
        # the customer account root folder id is taken from the tid. This may 
        # break in the future if the tid scheme changes. Another option would 
        # be to add a root folder to the class that would be set using the 
        # account id. For now, this is fine so go with it.
        name_pieces = ['',tid[:24]]
        for _ in range(3):
            name_pieces.append(rng.choice(string.ascii_letters))

        # Add the filename
        name_pieces.append(AkamaiHosting.neon_fname_fmt % 
                           (tid, image.size[0], image.size[1]))

        image_url = '/'.join(name_pieces)
        
        # the full cdn url
        cdn_url = "http://%s%s" % (cdn_prefix, image_url)

        # Get the image data
        fmt = 'jpeg'
        filestream = StringIO()
        image.save(filestream, fmt, quality=90) 
        filestream.seek(0)
        imgdata = filestream.read()

        response = yield tornado.gen.Task(self.ak_conn.upload, image_url,
                                          imgdata)
        if response.error:
            _log.warn_n("Error uploading image to akamai for tid %s" % tid)
            cdn_url = None

        raise tornado.gen.Return(cdn_url) 
