'''
Simple tests for gpu_video_searcher and _gpuPredictorLite
'''

'''
========================================================================
TEST CASE 1
- Single client
- Single GPU manager
- Single video
- Terminates after 5 seconds and requests results
'''


from glob import glob
import logging
from time import sleep

from _gpuPredictorLite import JobManager
from gpu_video_searcher import GPUVideoSearch
from gpu_video_searcher import MonteCarloMetropolisHastings
import numpy as np



logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)s][%(process)-10s][%(threadName)-10s][%(funcName)s] %(message)s',
                    )

MODEL_FILE = '/Users/ndufour/neon_caffe_models/deploy.prototxt'
PRETRAINED = '/Users/ndufour/neon_caffe_models/valence__iter_660000.caffemodel'
videos = glob('/Users/ndufour/target_videos/*')


logging.debug('Starting JobManager')

jm = JobManager(
    model_file=MODEL_FILE,
    pretrained_file=PRETRAINED)

logging.debug('Obtaining Predictor')
predictor = jm.register_client()

vs = GPUVideoSearcher(predictor, MonteCarloMetropolisHastings)

video_file = videos[0]

vs.choose_thumbnails(video_file, 10)
sleep(5)

results = vs.get_results()
