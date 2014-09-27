'''
Copyright Josh Gachnang 2012, josh@servercobra.com, released under the New BSD License.

SQS Mock Python is a mock implementation of the Amazon SQS API. It implements (most) of the API for Connections and Queues, and returns the appropriate values for API calls. 
It works by holding the queue on disk, and implementing the API calls to work with a file per queue, rather than a distributed system. Unlike SQS, it is able to work in real
time. When an object is added to the queue, it is instantly available, making testing much easier. Applications can simply make calls to the SQSConnectionMock class instead 
of the SQSConnection class for testing purposes. This is only desirable if the application you are writing is tolerant of the delays inherent in SQS. If not, you should
use something like RabbitMQ instead. 

Modified by Sunil 

'''
import os
import pickle
import tempfile
import threading

class SQSQueueMock:
    '''
    SQS Mock Queue class
    NOTE: Currently only supports creation & testing of a single Queue
    '''
    name = None
    def __init__(self, filename, create=False):
        self.lock = threading.RLock()

        try:
            open(filename)
        except IOError:
            if create:
                self.tempfile = tempfile.NamedTemporaryFile()
                open(self.tempfile.name, 'w')
            else:
                raise SyntaxError("Queue %s does not exist" % filename)
        self.name = filename
        
    def clear(self, page_size=10, vtimeout=10):
        try:
            open(self.tempfile.name, 'w').close()
        except EOFError:
            return False
        return True
    
    def count(self, page_size=10, vtimeout=10):
        try:
            with self.lock:
                prev_data = pickle.load(open(self.tempfile.name, 'r'))
        except EOFError:
            return 0
        return len(prev_data)
        
    def count_slow(self, page_size=10, vtimeout=10):
        return count(page_size=10, vtimeout=10)
        
    def delete(self):
        try:
            os.remove(self.tempfile.name)
        except OSError:
            # What happens here?
            return False
        return True

    def delete_message(self, message):
        with self.lock:
            prev_data = pickle.load(open(self.tempfile.name, 'r'))
        for data in prev_data:
            if data.get_body() == message.get_body():
                try:
                    prev_data.remove(data)
                    break
                except ValueError:
                    return False
        try:
            with self.lock:
                pickle.dump(prev_data, open(self.tempfile.name, 'w'))
        except IOError:
            return False
        
        return True

    def get_messages(self, num_messages=1, visibility_timeout=None, attributes=None):
        messages = []
        try:
            with self.lock:
                prev_data = pickle.load(open(self.tempfile.name, 'r'))
        except EOFError:
            prev_data = []
        i = 0
        while i < num_messages and len(prev_data) > 0:
            try:
                messages.append(prev_data[i])
            except IndexError:
                pass
            i += 1
        return messages

    def read(self, visibility_timeout=None):
        with self.lock:
            prev_data = pickle.load(open(self.tempfile.name, 'r'))
        try:
            return prev_data.pop(0)
        except IndexError:
            # Is this the real action?
            return None

    def write(self, message):
        # Should do some error checking
        # read in all the data in the queue first
        try:
            with self.lock:
                prev_data = pickle.load(open(self.tempfile.name, 'r'))
        except EOFError:
            prev_data = []

        prev_data.append(message)
        
        try:
            with self.lock:
                pickle.dump(prev_data, open(self.tempfile.name, 'w'))
        except IOError:
            return False
            
        return True
    
class SQSConnectionMock:
    # TODO(Sunil): Needs to support tempfile to use the queue methods here except for the create Q one
    def get_queue(self, queue):
        try:
            queue_file = open(queue + ".sqs")
        except IOError:
            return None
        try:
            return SQSQueueMock(queue)
        except SyntaxError:
            return None
            
    def get_all_queues(self, prefix=""):
        queue_list = []
        files = os.listdir(".")
        for f in files:
            if f[-4:] == '.sqs':
                if prefix != "":
                    if f[0:len(prefix)] == prefix:
                        try:
                            # Try to make the queue. If there's something wrong, just move on.
                            q = SQSQueueMock(f)
                        except SyntaxError:
                            continue
                        queue_list.append(q)
                else:
                    try:
                        # Try to make the queue. If there's something wrong, just move on.
                        q = SQSQueueMock(f[:-4])
                    except SyntaxError:
                        print 'err', f
                        continue
                    queue_list.append(q)
        return queue_list

    def delete_queue(self, queue, force_deletion=False):
        q = self.get_queue(queue)
        #print 'type', type(q)
        if q.count() != 0:
            # Can only delete empty queues
            return False
        return q.delete()
        
    def delete_message(self, queue, message):
        return queue.delete_message(message)
        
    def create_queue(self, name):
        a = SQSQueueMock(name, create=True)
        return a
