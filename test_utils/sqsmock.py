'''
Copyright Josh Gachnang 2012, josh@servercobra.com, released under the New BSD License.

SQS Mock Python is a mock implementation of the Amazon SQS API. It implements (most) of the API for Connections and Queues, and returns the appropriate values for API calls. 
It works by holding the queue on disk, and implementing the API calls to work with a file per queue, rather than a distributed system. Unlike SQS, it is able to work in real
time. When an object is added to the queue, it is instantly available, making testing much easier. Applications can simply make calls to the SQSConnectionMock class instead 
of the SQSConnection class for testing purposes. This is only desirable if the application you are writing is tolerant of the delays inherent in SQS. If not, you should
use something like RabbitMQ instead. 

Modified by Sunil
Modified by Ahmaidan

'''
from boto.sqs.message import Message
import random
from random import randrange
import threading

import logging
_log = logging.getLogger(__name__)

class SQSQueueMock(object):
    '''
    SQS Mock Queue class
    '''
    name = None  
    def __init__(self):
        self.lock = threading.RLock()

        self.attributes = {}
        self.message_queue = []
        
    #Deprecated function           
    def clear(self, page_size=10, vtimeout=10):
        try:
            del self.message_queue[:]
        except EOFError:
            return False
        return True
           
    def count(self, page_size=10, vtimeout=10):
        count = len(self.message_queue)
        return count
               
    def count_slow(self, page_size=10, vtimeout=10):
        return self.count(page_size=10, vtimeout=10)
       
    def delete(self):
        try:
            del self.message_queue[:]
        except OSError:
            return False
        return True
      
    def delete_message(self, message):
        with self.lock:
            try:
                for x in range(0, len(self.message_queue)):
                    if(message.receipt_handle == self.message_queue[x].receipt_handle):
                        message = self.message_queue.pop(x)
                        return True
            except ValueError:
                return False
        return False
       
    def get_messages(self, num_messages=1, visibility_timeout=None, attributes=None):
        messages = []

        i = 0
        while i < num_messages and len(prev_data) > 0:
            with self.lock:
                try:
                    msg = self.message_queue(i)
                    for attr in attributes_to_return:
                        try:
                            msg.attributes[attr] = self.attributes[attr]
                        except KeyError:
                            pass
                    messages.append(msg)
                except IndexError:
                    pass
                i += 1
        return messages
       
    def read(self, visibility_timeout=None, message_attributes=['All']):
        with self.lock:
            if self.message_queue:
                return self.message_queue[0]
            else:
                return None
    
    def write(self, message):
        try:
            with self.lock:
                self.message_queue.append(message)
        except IndexError:
            raise Exception()
        return message
    
class SQSConnectionMock(object):
    def __init__(self):
        self.queue_dict = {}
        self.queue_list = []
        self.regions = ['us-east-1', 'us-west-2', 'us-west-1',
                        'eu-west-1', 'eu-central-1', 'ap-southeast-1',
                        'ap-southeast-2', 'ap-northeast-1', 'sa-east-1']

    def connect_to_region(self, region):
        _log.info("Yes")
        if region not in self.regions:
            raise AttributeError

    def get_queue(self, queue_name):
        try:
            return self.queue_dict[queue_name]
        except SyntaxError:
            return None
             
    def get_all_queues(self, prefix=""):
        return self.queue_list
        
    def delete_queue(self, queue, force_deletion=False):
        q = self.get_queue(queue)
        if q.count() != 0:
            # Can only delete empty queues
            return False
        return q.close()
                
    def delete_message(self, queue, message):
        return queue.delete_message(message)
               
    def create_queue(self, name, visibility_timeout=None):
        q = SQSQueueMock()
        self.queue_dict[name] = q
        self.queue_list.append(q)
        return q

    def lookup(self, name):
        return self.queue_dict.get(name)

    #TODO: change this so it can actually approximate the functionality        
    def change_message_visibility(self, queue, receipt_handle, timeout):
        return True
