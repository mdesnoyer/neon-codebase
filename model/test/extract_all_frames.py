'''
The script extracts all frames of a video and associates them with scores.
'''
import threading
import sys
from time import sleep
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

scores = np.zeros(num_frames)

app_lock = threading.Lock()
res = []
def done(result_future, frameno):
    with app_lock:
        exception = result_future.exception()
        if exception:
            print 'Exception!'
            return
        else:
            result = result_future.result()
        res.append((frameno, result.valence[0]))

a = True
frameno = 0
while a:
    a, b = vid.read()
    result_future = predictor.predict(b)
    result_future.add_done_callback(
                    lambda result_future: done(result_future, frameno))
    print frameno
    frameno += 1

while len(res) < frameno:
    print 'Waiting for results to finish'
    sleep(2)

with open('/tmp/lemonade_results', 'w') as f:
    f.write(str(res))
    

