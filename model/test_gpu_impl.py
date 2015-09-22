'''
Test cases for the gpu Video Client.
'''
%load_ext autoreload
%autoreload 2

from glob import glob
from _gpuPredictorLite import JobManager
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
from _gpuPredictorLite import JobManager
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
p.join()
jm.stop()