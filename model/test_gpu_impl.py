'''
Test cases for the gpu Video Client.
'''
%load_ext autoreload
%autoreload 2

from glob import glob
from _gpuPredictor import JobManager
import numpy as np
import threading
from time import sleep 

MODEL_FILE = '/hdd/caffe/models/valence/deploy.prototxt'
PRETRAINED = '/hdd/caffe/models/valence/snaps/resize_snaps/valence__iter_660000.caffemodel'

images = glob('/hdd/other/target_images/*.jpg')

'''
===============================================================================================
Monoprocess version
===============================================================================================
'''

jm = JobManager(
    model_file=MODEL_FILE,
    pretrained_file=PRETRAINED)

predictor = jm.register_client()

vid = 0

for jid, image in enumerate(images):
    predictor.predict(image, vid, jid)

sleep(5)
predictor.stop()
jm.stop()


'''
===============================================================================================
Multiprocess version
===============================================================================================
'''
%load_ext autoreload
%autoreload 2

from glob import glob
from _gpuPredictor import JobManager
import numpy as np
import threading
from time import sleep 
from multiprocessing import Process

MODEL_FILE = '/hdd/caffe/models/valence/deploy.prototxt'
PRETRAINED = '/hdd/caffe/models/valence/snaps/resize_snaps/valence__iter_660000.caffemodel'

images = glob('/hdd/other/target_images/*.jpg')

def issue_job_requests(predictor, images):
    for jid, image in enumerate(images*3):
        predictor.predict(image, 0, jid)
    sleep(10)
    print predictor.results()
    predictor.stop()

jm = JobManager(
    model_file=MODEL_FILE,
    pretrained_file=PRETRAINED,
    N=33)

predictor = jm.register_client()

p = Process(target=issue_job_requests,
            name='requester client',
            args=(predictor, images))

p.start()
p.join(5)
jm.stop()

'''
===============================================================================================
Searching Videos
===============================================================================================
'''

# this will attempt to search through videos 
%load_ext autoreload
%autoreload 2
from glob import glob
from _gpuPredictor import JobManager
import numpy as np
import threading
from time import sleep, time 
from multiprocessing import Process
from gpu_video_searcher import GPUVideoSearch, MonteCarloMetropolisHastings
import logging

MODEL_FILE = '/hdd/caffe/models/valence/deploy.prototxt'
PRETRAINED = '/hdd/caffe/models/valence/snaps/resize_snaps/valence__iter_660000.caffemodel'

logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)s][%(process)-10s][%(threadName)-10s][%(funcName)s] %(message)s',
                    )
videos = glob('/hdd/other/test_videos/*')

def GVSWrapper(predictor, video_file, waitt=5):
    '''
    Wraps the GPU Video Searcher and dispatches jobs.
    '''
    logging.debug('Starting GVS')
    gvs = GPUVideoSearch(predictor, MonteCarloMetropolisHastings)
    logging.debug('Searching video %s'%video_file)
    gvs.choose_thumbnails(video_file, 10)
    logging.debug('Waiting for %i seconds'%waitt)
    s = time()
    while (time() - s) < waitt:
        logging.debug('%.2f remaining'%(time() - s))
        sleep(1)
    logging.debug('Getting results')
    res = gvs.get_result()
    print res

jm = JobManager(
    model_file=MODEL_FILE,
    pretrained_file=PRETRAINED,
    N=1000)

predictor = jm.register_client()

procs = []
for video_file in videos:
    procs.append(Process(target=GVSWrapper,
                name='video_search_wrapper_1',
                args=(predictor, video_file,
                    np.random.randint(3,15))))
    procs[-1].start()
    sleep(np.random.randint(5))
    break

for proc in procs:
    proc.join(5)

jm.stop()

'''
===============================================================================================
Searching Multiple Videos with a Single Client
===============================================================================================
'''

# this will attempt to search through videos 
%load_ext autoreload
%autoreload 2
from glob import glob
from _gpuPredictor import JobManager
import numpy as np
import threading
from time import sleep, time 
from multiprocessing import Process
from gpu_video_searcher import GPUVideoSearch, MonteCarloMetropolisHastings
import logging

MODEL_FILE = '/hdd/caffe/models/valence/deploy.prototxt'
PRETRAINED = '/hdd/caffe/models/valence/snaps/resize_snaps/valence__iter_660000.caffemodel'

logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)s][%(process)-10s][%(threadName)-10s][%(funcName)s] %(message)s',
                    )
videos = glob('/hdd/other/test_videos/*')

def GVSWrapper(predictor, videos, waitt=5):
    '''
    Wraps the GPU Video Searcher and dispatches jobs.
    '''
    logging.debug('Starting GVS')
    gvs = GPUVideoSearch(predictor, MonteCarloMetropolisHastings)
    for video_file in videos[:2]:
        logging.debug('Searching video %s'%video_file)
        gvs.choose_thumbnails(video_file, 10)
        logging.debug('Waiting for %i seconds'%waitt)
        s = time()
        while (time() - s) < waitt:
            logging.debug('%.2f remaining'%(time() - s))
            sleep(1)
        logging.debug('Getting results')
        res = gvs.get_result()
        print res
    gvs.stop()

jm = JobManager(
    model_file=MODEL_FILE,
    pretrained_file=PRETRAINED,
    N=1000)

predictor = jm.register_client()

p1=(Process(target=GVSWrapper,
            name='video_search_wrapper_1',
            args=(predictor, videos,5)))
p1.start()
predictor.stop()
p1.join(70)

jm.stop()

'''
===============================================================================================
Searching Videos - Validate the obtained scores
===============================================================================================
'''
# this will attempt to search through videos 
from glob import glob
from _gpuPredictor import JobManager
import numpy as np
import threading
from time import sleep, time 
from multiprocessing import Process
from gpu_video_searcher import GPUVideoSearch, MonteCarloMetropolisHastings
import logging

MODEL_FILE = '/hdd/caffe/models/valence/deploy.prototxt'
PRETRAINED = '/hdd/caffe/models/valence/snaps/resize_snaps/valence__iter_660000.caffemodel'

logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)s][%(process)-10s][%(threadName)-10s][%(funcName)s] %(message)s',
                    )
videos = glob('/hdd/other/test_videos/*')
videos = ['/hdd/other/test_videos/test2.mp4']

def GVSWrapper(predictor, videos, waitt=5):
    '''
    Wraps the GPU Video Searcher and dispatches jobs.
    '''
    logging.debug('Starting GVS')
    gvs = GPUVideoSearch(predictor, MonteCarloMetropolisHastings)
    for video_file in videos[:2]:
        logging.debug('Searching video %s'%video_file)
        gvs.choose_thumbnails(video_file, 10)
        logging.debug('Waiting for %i seconds'%waitt)
        s = time()
        while (time() - s) < waitt:
            logging.debug('%.2f remaining'%(time() - s))
            sleep(1)
        logging.debug('Getting results')
        res = gvs.get_result()
        print res
    gvs.stop()

jm = JobManager(
    model_file=MODEL_FILE,
    pretrained_file=PRETRAINED,
    N=1000)

predictor = jm.register_client()

# p1=(Process(target=GVSWrapper,
#             name='video_search_wrapper_1',
#             args=(predictor, videos,20)))
# p1.start()
# predictor.stop()
# p1.join(70)

# # let's try running it locally

GVSWrapper(predictor, videos, 20)
jm.stop()
