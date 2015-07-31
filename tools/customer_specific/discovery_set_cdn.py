#!/usr/bin/env python
'''
Script that submits a lot of Discovery jobs for backprocessing

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright Neon Labs 2015
'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import cmsdb.cdnhosting
from cmsdb import neondata
import utils.neon
import utils.imageutils

s3cdn = neondata.S3CDNHostingMetadata(
    bucket_name='fusion.ddmcdn.com', 
    cdn_prefixes=['http://fusion.ddmcdn.com'],
    folder_prefix='neon', resize=True, 
    update_serving_urls=True,
    rendition_sizes=[[221,124],[440,248],[640,360],[133,75],[334,223],[336,336],[337,337],[310,465],[280,900],[600,355]],
    policy='public-read')


hoster = cmsdb.cdnhosting.CDNHosting.create(s3cdn)

im = utils.imageutils.PILImageUtils.create_random_image(480,640)

print hoster._upload_impl(im, 'testtid2')
