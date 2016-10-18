#!/usr/bin/env python
'''
Script that extracts the features for frames in a video.

Outputs a pandas pickle where each column is a (url, frame number) and
each row is a feature vector value.

Author: Mark Desnoyer (desnoyer@neon-lab.com)
Copyright 2016 Neon Labs Inc.
'''
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import cv2
import logging
import model.predictor
import pandas as pd
import re
import tornado.gen
import utils.autoscale
import utils.neon
import utils.pycvutils
import utils.sync
import utils.video_download
from utils.options import options, define

_log = logging.getLogger(__name__)
define('input', default=None,
       help='URL of the video')
define('output', default=None,
       help='Output file, which will be a pandas pickle')
define('aq_groups', default='AquilaOnDemandTest,AquilaTestSpot',
       help=('Comma separated list of autoscaling groups to talk to for '
             'aquilla'))
define('frame_step', default=10, 
       help='Number of frames to step between samples')

class LocalConn(object):
    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def get_ip(self, force_refresh=False):
        raise tornado.gen.Return('localhost')

@utils.sync.optional_sync
@tornado.gen.coroutine
def main(video_url, outfn):
    conn = utils.autoscale.MultipleAutoScaleGroups(
        options.aq_groups.split(','))
    #conn = LocalConn()
    predictor = model.predictor.DeepnetPredictor(aquila_connection=conn)
    
    vid_downloader = utils.video_download.VideoDownloader(video_url)
    yield vid_downloader.download_video_file()
    vid = cv2.VideoCapture(vid_downloader.get_local_filename())

    try:
        predictor.connect()

        futs = []
        data = []
        frameno=0
        for frame in utils.pycvutils.iterate_video(vid,
                                                   step=options.frame_step):
            futs.append(predictor.predict(frame, async=True))
            if len(futs) >= 10:
                res = yield futs
                for score, features, version in res:
                    data.append(pd.Series(features, name=(video_url, frameno)))
                    frameno += options.frame_step

        res = yield futs
        for score, features, version in res:
            data.append(pd.Series(features, name=(video_url, frameno)))
            frameno += options.frame_step

    finally:
        predictor.shutdown()

    data = pd.concat(data, axis=1)
    data.to_pickle(outfn)
    _log.info('Output file to %s' % outfn)
    

if __name__ == '__main__':
    utils.neon.InitNeon()
    logging.getLogger('boto').propagate = False
    main(options.input, options.output)
