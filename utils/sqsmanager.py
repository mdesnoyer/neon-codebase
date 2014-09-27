'''
SQS Manager 

Contains class to manage Customer callbacks
'''

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

import boto.sqs
from boto.sqs.message import Message
from boto.sqs.jsonmessage import JSONMessage
from boto.exception import SQSError
import json
import logging
import random
import threading
import tornado.httpclient
import utils.http
import time
from utils.options import define, options

define('region', type=str, default="us-east-1", help='region to connect to')

_log = logging.getLogger(__name__)

class CustomerCallbackMessage(object):
    '''
    Neon customer callback message class that is to be 
    stored in Amazon SQS 
    '''
    def __init__(self, video_id, url, response, dtime=None):
        self.video_id = video_id 
        self.callback_url = url
        self.response = response
        # If dispatch_time is not None, there has been a thread created
        # to deliver the callback message
        self.dispatch_time = dtime 

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)
    
    @classmethod
    def to_obj(cls, json_data):
        if not json_data:
            return
        data = json.loads(json_data)
        return CustomerCallbackMessage(data['video_id'], data['callback_url'],
                                        data['response'],
                                        data['dispatch_time'])

class SQSManager(object):
    def __init__(self, sqs_name):
        # should connect with server HMAC credentials
        conn = boto.connect_sqs()
                            #'AKIAJ5G2RZ6BDNBZ2VBA',
                            #'d9Q9abhaUh625uXpSrKElvQ/DrbKsCUAYAPaeVLU')

        # create_queue method will create (and return) the requested queue if it
        # does not exist or will return the existing queue if it does.
        self.sq = conn.create_queue(sqs_name)
        self.messages = [] 
        self.lock = threading.RLock()

    def get_all_messages(self):
        '''
        Get all the message currently in the Q
        '''
        count = self.sq.count()
        # NOTE: Page size must be <= 10 else SQS errors
        while count > 0:
            rs = self.sq.get_messages(10)
            self.messages.extend(rs)
            count -= len(rs)
        return self.messages

    def remove_message(self, msg):
        ''' Delete the message from SQS Queue
        '''
        # Note: the lock isnt' required for delete_message, but since 
        # sqsmock uses file i/o we require a lock to serialize the deletions 
        with self.lock:
            if self.sq.delete_message(msg):
                self.messages.remove(msg)

class CustomerCallbackManager(SQSManager):
    '''
    Manages the addition, deletion of callbacks in SQS
    And also sends the callback respone to the customer
    '''

    region = 'us-east-1'
    sqs_name = 'neon-customer-callback'
    delay = 60

    def __init__(self):
        SQSManager.__init__(self, CustomerCallbackManager.sqs_name)
        self.callback_messages = {} # platform video_id => CustomerCallbackMessage obj 
        self.n_callbacks = 0

    def add_callback_response(self, video_id, callback_url, response):
        '''
        @key : video_id
        @response: json response to be sent to the customer
        '''

        cbm = CustomerCallbackMessage(video_id, callback_url, response)
        msg = Message()
        msg.set_body(cbm.to_json())
        try:
            self.sq.write(msg)
        except SQSError, e:
            _log.error("failed to write to SQS")
            return False
        
        return True
   
    def get_callback_messages(self):
        '''
        Get all the messages from the Q and 
        return the CustomerCallbackMessages
        '''
        msgs = self.get_all_messages()
        for msg in msgs:
            ccm = CustomerCallbackMessage.to_obj(msg.get_body())
            try:
                self.callback_messages[ccm.video_id]
            except KeyError, e:
                #If key not present add to the dict
                self.callback_messages[ccm.video_id] = ccm

        return self.callback_messages.values()

    def get_callback_message(self, key):
        '''
        Get callback response given a key
        '''
        try:
            return self.callback_messages[key]
        except KeyError, e:
            return None
    
    def remove_callback(self, video_id):
        for msg in self.messages:
            ccm = CustomerCallbackMessage.to_obj(msg.get_body())
            if ccm.video_id == video_id:
                #remove from SQS Queue 
                self.remove_message(msg)

    def send_response_and_remove_message(self, ccm, request):
        '''
        Makes HTTP call to send the callback response and removes
        the message from the SQS Queue

        '''
        response = utils.http.send_request(request)
        if not response.error:
            _log.info("Callback completed for vid %s" % ccm.video_id)
            self.remove_callback(ccm.video_id)
            with self.lock:
                self.callback_messages.pop(ccm.video_id)
                self.n_callbacks -= 1

    def send_callback_response(self, key):
        '''
        Send callback to the customer
        '''
        with self.lock:
            self.n_callbacks += 1

        ccm = self.get_callback_message(key)
        url = ccm.callback_url
        body = ccm.response

        headers = {"Content-Type": "application/json"}
        request = tornado.httpclient.HTTPRequest(
                            url=url,
                            method="POST",
                            body=body, 
                            headers=headers,
                            request_timeout=20.0, 
                            connect_timeout=10.0)
        # Stagger the threads
        delay = random.randint(CustomerCallbackManager.delay - 10,
                                CustomerCallbackManager.delay)
        ccm.dispatch_time = time.time() + delay
        t = threading.Timer(delay,
                            self.send_response_and_remove_message, 
                            [ccm, request])
        t.start()

    def schedule_all_callbacks(self, mastermind_video_ids):
        '''
        @mastermind_video_ids : video ids that mastermind has generated 
        directives for
        '''
        
        # Populate all the callbacks from SQS
        self.get_callback_messages()

        # For every video id & its callback response that has been added to SQS, then
        # check if mastermind has written a directive for that video id. If yes,
        # then schedule to send the callback response to the customer
       
        for vid in self.callback_messages.keys():
            if vid in mastermind_video_ids:
                if self.callback_messages[vid].dispatch_time is None:
                    self.send_callback_response(vid)
                    _log.info("Scheduling the callback for vid %s" % vid)
