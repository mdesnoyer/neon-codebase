import os.path
import sys
import numpy as np
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

w2c_data = np.load('w2c.dat')
w2c_data = w2c_data[0:, 3:]
w2c_max = np.argmax(w2c_data, axis=1)
print w2c_max

import cv2

class ColorName(object):
    '''For a given image, returns the colorname histogram.'''    
    def __init__(self, image):
    	self.image = image

    # order of color names: black, blue, brown, grey, green,
    #                       orange, pink, purple, red, white, yellow

    def colorname_to_color(self):
        self._image_to_colorname()
        color_values = np.array(
                        [[0, 0, 0], [1, 0, 0], [.25, .4, .5], [.5, .5, .5],
                        [0, 1, 0], [0, .8, 1], [1, .5, 1], [1, 0, 1],
                        [0, 0, 1] , [1, 1, 1], [0, 1, 1]])
        new_color_image = color_values[self.colorname_image]*255
        cv2.imshow('win', new_color_image)
        ret = cv2.waitKey()

    def _image_to_colorname(self):
        BB = self.image[0:, 0:, 0] / 8
        GG = self.image[0:, 0:, 1] / 8
        RR = self.image[0:, 0:, 2] / 8
        index_im = RR + 32 * GG + 32 * 32 * BB
        self.colorname_image = w2c_max[index_im]

    def get_colorname_histogram(self):
        self._image_to_colorname()
        hist_result = np.histogram(self.colorname_image)[0]
        normalized_hist = hist_result.astype(float)/sum(hist_result)
        return normalized_hist

def main():
    image_1 = cv2.imread('/home/wiley/Pictures/face.jpg')
    image_2 = cv2.imread('/home/wiley/Downloads/ColorNaming/car.jpg')
    cn_2 = ColorName(image_2)
    cn_2.colorname_to_color()




if __name__ == "__main__":
    main()

# import atexit
# import logging
# import os
# import os.path
# import random
# import signal
# import socket
# import subprocess
# import sys
# import urllib
# from glob import glob
# import boto.opsworks
# import code
# import cv2
# import ipdb
# import utils.logs as logs
# import utils.neon
# import utils.ps
# from cmsdb.neondata import *
# from model import add_filter_to_model
# from model._model import load_model
# from model.show_top_thumbnails import run_one_video
# from utils import logs as logs
# from utils.neon import EnableRunningDebugging
# from utils.options import define, options
# _log = logging.getLogger(__name__)
# aws_region = "us-east-1"
# layer_name = "dbslave"
# stack_name = "Neon Serving Stack V2"
# redis_port = 6379
# forward_port = 1
# def find_db_address():
#     conn = boto.opsworks.connect_to_region(aws_region)
#     # Find the stack
#     stack_id = None
#     for stack in conn.describe_stacks()['Stacks']:
#         if stack['Name'] == stack_name:
#             stack_id = stack['StackId']
#             break
#     if stack_id is None:
#         raise Exception('Could not find stack %s' % stack_name)
#     # Find the layer
#     layer_id = None
#     for layer in conn.describe_layers(stack_id = stack_id)['Layers']:
#         if layer['Shortname'] == layer_name:
#             layer_id = layer['LayerId']
#             break
#     if layer_id is None:
#         raise Exception('Could not find layer %s in stack %s' %
#                         (layer_name, stack_name))
#     # Find the instance ip
#     ip = None
#     for instance in conn.describe_instances(layer_id=layer_id)['Instances']:
#         try:
#             ip = instance['PrivateIp']
#             break
#         except KeyError:
#             pass
#     _log.info('Found db at %s' % ip)
#     return ip
# def forward_port(local_port):
#     cmd = ('ssh -L {local}:localhost:{rem} {db_addr}').format(
#                local=local_port,
#                rem=redis_port,
#                db_addr=find_db_address())
#     _log.info('Forwarding port %s to the database' % local_port)
#     proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
#                             stderr=subprocess.STDOUT,
#                             stdin=subprocess.PIPE)
#     # Wait until we can connect to the db
#     for i in range(12):
#         try:
#             sock = socket.create_connection(('localhost', local_port), 3600)
#         except socket.error:
#             time.sleep(5)
#     if sock is None:
#         raise Exception('Could not connect to the database')
    
#     _log.info('Connection made to the database')
#     sock.shutdown(socket.SHUT_RDWR)
#     sock.close()
    
#     return proc
# logs.AddConfiguredLogger()
# EnableRunningDebugging()
# atexit.register(utils.ps.shutdown_children)
# signal.signal(signal.SIGTERM, lambda sig, y: sys.exit(-sig))
# random.seed()
# proc = None
# if forward_port:
#     db_address = 'localhost'
#     db_port = random.randint(10000, 12000)
#     proc = forward_port(db_port)
# else:
#     db_address = find_db_address()
#     db_port = redis_port
# # Set the options for connecting to the db
# options._set('cmsdb.neondata.dbPort', db_port)
# options._set('cmsdb.neondata.accountDB', db_address)
# options._set('cmsdb.neondata.thumbnailDB', db_address)
# options._set('cmsdb.neondata.videoDB', db_address)
# print 'Loading model'
# model = load_model('/data/model_data/p_20150902_bigModel.model')
# print 'Fetching all user accounts'
# accounts = NeonUserAccount.get_all()
# bdest = '/data/filtering_crisis/bad_images'
# gdest = '/data/filtering_crisis/good_images'
# def im_from_url(url):
#     req = urllib.urlopen(url)
#     arr = np.asarray(bytearray(req.read()), dtype=np.uint8)
#     img = cv2.imdecode(arr,-1) # 'load it as it is'
#     # resize so that it's not gigantic
#     # let's do this by fixing the maximum size to 1000
#     retv = 1000./np.max(img.shape)
#     rimg = cv2.resize(img, (int(img.shape[1]*retv), int(img.shape[0]*retv)))
#     cv2.imshow('win', rimg)
#     ret = cv2.waitKey()
#     if ret == ord('y'):
#         cv2.imwrite(os.path.join(bdest, tmd.key + '.jpg'), img)
#     if ret == ord('n'):
#         cv2.imwrite(os.path.join(gdest, tmd.key + '.jpg'), img)
#     return ret
# cbreak = False
# skipAcct = False
# good = 0
# bad = 0
# for n, account in enumerate(accounts):
#     print '%i/%i: %s'%(n, len(accounts), account.account_id)
#     for video in account.iterate_all_videos():
#         if video.model_version == 'p_20150902_bigModel':
#             print '\t' + video.get_id()
#             # then you can iterate through the thumbnails
#             for thumb in video.thumbnail_ids:
#                 tmd = ThumbnailMetadata.get(thumb)
#                 print '\t\t' + tmd.get_id()
#                 ret = im_from_url(tmd.urls[0])
#                 if ret == ord('q'):
#                     cv2.destroyAllWindows()
#                     cbreak = True
#                     break
#                 elif ret == ord('s'):
#                     skipAcct = True
#                     break
#                 elif ret == ord('y'):
#                     bad += 1
#                 elif ret == ord('n'):
#                     good += 1
#                 print '\t\t\tGood: %i, Bad: %i'%(good, bad)
#         if cbreak:
#             break
#         if skipAcct:
#             skipAcct = False
#             break
#     if cbreak:
#         break
# cv2.destroyAllWindows()

