'''
SQS Manager 

Contains class to manage Customer callbacks
'''

import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

import boto.sqs
from boto.sqs.message import Message
from boto.sqs.jsonmessage import JSONMessage
import boto.exception
import copy
import json
import logging
import random
import statemon
import threading
import tornado.gen
import tornado.httpclient
import utils.http
import utils.sync
import time
from utils.options import define, options

define('region', type=str, default="us-east-1", help='region to connect to')
define('customer_callback_sqs', type=str, default="neon-customer-callback",
       help='SQS queue name')

statemon.define('callbacks_in_flight', int)
statemon.define('callback_errors', int)
statemon.define('sucessful_callbacks', int)

_log = logging.getLogger(__name__)

class CustomerCallbackMessage(object):
    '''
    Neon customer callback message class that is to be 
    stored in Amazon SQS 
    '''
    def __init__(self, video_id, url, response, dtime=None, receive_count=0):
        self.video_id = video_id 
        self.callback_url = url
        self.response = response
        # If dispatch_time is not None, there has been a thread created
        # to deliver the callback message
        self.dispatch_time = dtime

        # The number of times the message has been pulled from the queue
        self.receive_count = receive_count

    def __cmp__(self, other):
        return cmp(self.__dict__, other.__dict__)

    def __str__(self):
        return self.to_json()

    def __repr__(self):
        return str(self)

    def to_json(self):
        return json.dumps(self.__dict__)
    
    @classmethod
    def to_obj(cls, json_data, attributes):
        if not json_data:
            return
        data = json.loads(json_data)
        return CustomerCallbackMessage(
            data['video_id'],
            data['callback_url'],
            data['response'],
            data['dispatch_time'],
            int(attributes.get('ApproximateReceiveCount', 0)))

class SQSManager(object):
    def __init__(self, sqs_name, visibility_timeout=None):
        # should connect with server HMAC credentials
        conn = boto.connect_sqs()

        self.visibility_timeout = visibility_timeout

        # create_queue method will create (and return) the requested
        # queue if it does not exist or will return the existing queue
        # if it does.
        self.sq = conn.create_queue(sqs_name, visibility_timeout)
        self.messages = [] 
        self.lock = threading.RLock()

    @tornado.gen.coroutine
    def get_all_messages(self):
        '''
        Get all the message currently in the Q
        '''
        # TODO: Make the calls to sqs asynchronous
        count = self.sq.count()
        # NOTE: Page size must be <= 10 else SQS errors
        try:
            while count > 0:
                rs = self.sq.get_messages(
                    10,
                    visibility_timeout=self.visibility_timeout,
                    attributes='ApproximateReceiveCount')
                with self.lock:
                    self.messages.extend(rs)
                count -= len(rs)
        except boto.exception.SQSDecodeError as e:
            _log.error('Unable to decode sqs message. Should never get here')
            tornado.gen.Return(self.messages)
        raise tornado.gen.Return(self.messages)

    @tornado.gen.coroutine
    def remove_message(self, msg):
        ''' Delete the message from SQS Queue
        '''
        # Note: the lock isnt' required for delete_message, but since 
        # sqsmock uses file i/o we require a lock to serialize the deletions 
        # TODO: Make this call asynchronous
        with self.lock:
            self.sq.delete_message(msg)
            try:
                self.messages.remove(msg)
            except ValueError:
                pass

class CustomerCallbackManager(SQSManager):
    '''
    Manages the addition, deletion of callbacks in SQS
    And also sends the callback respone to the customer
    '''

    region = 'us-east-1'

    def __init__(self, visibility_timeout=None, max_callback_tries=3):
        SQSManager.__init__(self, options.customer_callback_sqs,
                            visibility_timeout)
        self.callback_messages = {} # platform video_id => CustomerCallbackMessage obj 
        self.max_callback_tries = max_callback_tries
        self.key_failures = {} # key => error_count

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def add_callback_response(self, key, callback_url, response):
        '''
        @key : external video_id
        @response: json response to be sent to the customer
        '''

        cbm = CustomerCallbackMessage(key, callback_url, response)
        msg = Message()
        msg.set_body(cbm.to_json())
        try:
            self.sq.write(msg)
        except boto.exception.SQSError as e:
            _log.error("failed to write to SQS: %s" % e)
            raise tornado.gen.Return(False)
        except boto.exception.BotoServerError as e:
            _log.error("failed to write to SQS: %s" % e)
            raise tornado.gen.Return(False)
        
        raise tornado.gen.Return(True)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def schedule_all_callbacks(self, keys):
        '''Schedules the callbacks to be sent for a number of keys
        
        @keys : list of keys that we should send callbacks for if 
                there is one that hasn't been sent in the past.
        '''
        
        # Populate all the callbacks from SQS
        yield self.get_callback_messages()

        # For every video id & its callback response that has been
        # added to SQS, then check if mastermind has written a
        # directive for that video id. If yes, then schedule to send
        # the callback response to the customer
        keys = set(keys)

        futures = []
        with self.lock:
            for key in self.callback_messages.keys():
                if key in keys:
                    if self.callback_messages[key].dispatch_time is None:
                        futures.append(self.send_callback_response(key))
                        _log.info("Scheduling the callback for key %s" % key)
        yield futures

    @tornado.gen.coroutine
    def get_callback_messages(self):
        '''
        Get all the messages from the SQS Q and 
        return the CustomerCallbackMessages
        '''
        msgs = yield self.get_all_messages()
        for msg in msgs:
            try:
                ccm = CustomerCallbackMessage.to_obj(msg.get_body(),
                                                     msg.attributes)
                with self.lock:
                    self.callback_messages.setdefault(ccm.video_id, ccm)
            except ValueError:
                _log.error('Bad message in SQS. removing: %s' % msg.get_body())
                self.sq.delete_message(msg)

        raise tornado.gen.Return(self.callback_messages.values())

    @tornado.gen.coroutine
    def remove_callback(self, key):
        '''Remove the callback from the SQS queue.
        '''
        messages = copy.copy(self.messages)
        for msg in messages:
            ccm = CustomerCallbackMessage.to_obj(msg.get_body(),
                                                 msg.attributes)
            if ccm.video_id == key:
                #remove from SQS Queue 
                yield self.remove_message(msg)

                # remove from messages list stored locally
                try:
                    self.messages.remove(msg)
                except ValueError:
                    pass
                

    @tornado.gen.coroutine
    def send_callback_response(self, key):
        '''
        Send callback to the customer for a message with a given key
        '''
        statemon.state.increment('callbacks_in_flight')
        try:
            with self.lock:
                try:
                    ccm = self.callback_messages[key]
                except KeyError as e:
                    _log.warn('No knowlege of callback for key %s ignoring' %
                              key)
                    return
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
            response = yield tornado.gen.Task(utils.http.send_request,
                                              request)
            if response.error is not None:
                prev_failures = self.key_failures.get(key, 0)
                if prev_failures >= self.max_callback_tries:
                    statemon.state.increment('callback_errors')
                    _log.error('Too many errors sending callback for key %s %s'
                               % (key, response.error))
                    del self.key_failures[key]
                    self.callback_messages.pop(key)
                    yield self.remove_callback(key)
                else:    
                    _log.warn('Error sending callback for key %s to %s' % 
                              (key, url))
                    self.key_failures[key] = prev_failures + 1
            else:
                _log.info('Callback completed for key %s' % key)
                statemon.state.increment('sucessful_callbacks')
                yield self.remove_callback(key)
                with self.lock:
                    self.callback_messages.pop(key)
                    try:
                        del self.key_failures[key]
                    except KeyError:
                        pass

        finally:
            statemon.state.decrement('callbacks_in_flight')
