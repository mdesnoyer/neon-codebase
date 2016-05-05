'''
This script extracts one frame per second from a video.
'''
import threading
import sys
from time import sleep
import cv2
import numpy as np
if '/opt/neon/neon-codebase/core/model' not in sys.path:
    sys.path.insert(0, '/opt/neon/neon-codebase/core/model')
    sys.path.insert(0, '/opt/neon/neon-codebase/core')
from model import predictor
predictor = predictor.DeepnetPredictor(hostport='10.0.66.209:9000',
                                       concurrency=66)

# read in the video, determine the number of frames, and preallocate
# an array to store them.
video = '/home/ubuntu/lemonade.m4v'
vid = cv2.VideoCapture(video)

fps = vid.get(cv2.CAP_PROP_FPS)
fpsi = int(np.round(fps))

app_lock = threading.Lock()
res = []
def done(result_future, frameno):
    with app_lock:
        exception = result_future.exception()
        if exception:
            print 'Exception!', exception.message
            return
        else:
            result = result_future.result()
        res.append((frameno, result.valence[0]))
        print frameno

a = True
a, b = vid.read()
tot = 0
while a:
    frameno = vid.get(cv2.CAP_PROP_POS_FRAMES)
    result_future = predictor.predict(b)
    result_future.add_done_callback(
                    lambda result_future: done(result_future, frameno))
    tot += 1
    a, b = vid.read()
    vid.set(cv2.CAP_PROP_POS_FRAMES, frameno + fpsi)

while len(res) < tot:
    print 'Waiting for results to finish'
    sleep(2)

with open('/tmp/lemonade_results', 'w') as f:
    f.write(str(res))

predictor.exit()