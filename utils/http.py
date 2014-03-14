'''A module full of http tools

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
sys.path.insert(0,os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

import logging
import Queue
import threading
import time
import tornado.escape
import tornado.httpclient
import tornado.ioloop

_log = logging.getLogger(__name__)

# TODO(mdesnoyer): Handle the stack on async requests so that the
# callback will have a stack that looks like the original request
# being called.

def send_request(request, ntries=5, callback=None, cur_try=0):
    '''Sends an HTTP request with retries

    If there was an error, either in the connection, or if the response is json and has a non-nill "error" field, the response.error will be a tornado.httpclient.HTTPError

    The retries occur with exponential backoff

    request - A tornado.httpclient.HTTPRequest object
    ntries - Number of times to try sending the request
    callback - If it is a function, it will be called when the request
               returns with its response as the parameter. If it is None,
               this call blocks and returns the HTTPResponse.

    '''
    def finish_request(response):
        if callback is not None:
            callback(response)
        return response

    def handle_response(response, cur_try):

        # Logic to identify errors
        if not response.error:
            try:
                data = tornado.escape.json_decode(response.body)
                if isinstance(data, dict) and data['error']:
                    _log.warning(('key=http_response_error '
                                  'msg=Response err from %s: %s') %
                                  (request.url, data['error']))
                else:
                    return finish_request(response)

                                
            except ValueError:
                # It's not JSON data so just call the callback
                return finish_request(response)

            except KeyError:
                # The JSON doens't have an error field, so
                # call the callback
                return finish_request(response)

        else:
            _log.warning(('key=http_connection_error '
                           'msg=Error connecting to %s: %s') %
                           (request.url, response.error))

        # Handling the retries
        cur_try += 1
        if cur_try >= ntries:
            response.error = tornado.httpclient.HTTPError(
                503, 'Too many errors connecting to %s' % request.url)
            return finish_request(response)


        delay = (1 << cur_try) * 0.1 # in seconds
        if callback is None:
            time.sleep(delay)
            return send_request(request, ntries, cur_try=cur_try)
        else:
            ioloop = tornado.ioloop.IOLoop.current()
            ioloop.add_callback(ioloop.add_timeout, time.time()+delay,
                                lambda: send_request(request, ntries, callback,
                                                     cur_try))

        # TODO(mdesnoyer): Return a future
        return None

    if callback is None:
        http_client = tornado.httpclient.HTTPClient()
        try:
            response = http_client.fetch(request)
        except tornado.httpclient.HTTPError as e:
            if e.response:
                response = e.response
            else:
                response = tornado.httpclient.HTTPResponse(request,
                                                           e.code,
                                                           error=e)
        return handle_response(response, cur_try)
    else:
        http_client = tornado.httpclient.AsyncHTTPClient()
        return http_client.fetch(request,
                                 callback=lambda x: handle_response(x, cur_try))

class RequestThread(threading.Thread):
    '''A thread that serially sends http requests.'''
    def __init__(self, q, max_tries):
        '''Constructor

        q - A Queue.Queue that this thread will consume from.
            Entries in teh queue are expected to be tuples of
            (request, callback, ntries) 
        max_retries - The maximum number of retries per request.
        '''
        super(RequestThread, self).__init__()
        self.q = q
        self.max_tries = max_tries
        self.daemon = True
        self._stopped = threading.Event()

    def stop(self):
        self._stopped.set()
        self.q.put((None, None, None))

    def run(self):
        while not self._stopped.is_set():
            try:
                request, callback, ntries = self.q.get()

                if request is None:
                    self.q.task_done()
                    if not self._stopped.is_set():
                        # This stop was for somebody else who is
                        # listening on the queue, so requeue it.
                        self.q.put((None, None, None))
                    continue

                response = send_request(request, ntries=1)
                if response.error is not None:
                    # Do retry logic
                    if (ntries + 1) >= self.max_tries:
                        _log.error(('key=http_too_many_errors '
                                    'msg=Abort. Too many errors for %s '
                                    'request to %s with body: %s')
                                    % (request.method, request.url,
                                       request.body))
                        callback(response)
                        self.q.task_done()
                    else:
                        delay = (1 << ntries) * 0.1 # in seconds
                        ntries += 1
                        self._delayed_requeue(request, callback, ntries, delay)
                else:
                    callback(response)
                    self.q.task_done()
                        

            except Exception as e:
                _log.exception(
                    'key=http_connection msg=Unhandled exception: %s'
                    % e)
                # Some error happened and we don't want to deadlock,
                # so flag it being done.
                self.q.task_done()

    def _delayed_requeue(self, request, callback, ntries, delay):
        '''Adds a request to the queue after delay seconds.'''
        def do_requeue():
            self.q.put((request, callback, ntries))
            self.q.task_done()

        timer = threading.Timer(delay, do_requeue)
        timer.start()

class RequestPool(object):
    '''Handles a number of concurrent requests to an http service.

    Ensures that target isn't hit too hard too quickly.
    Includes exponential retries.
    '''
    def __init__(self, max_connections=1, max_tries=5):
        self.request_q = Queue.Queue()

        self.threads = []
        for i in range(max_connections):
            thread = RequestThread(self.request_q, max_tries)
            thread.start()
            self.threads.append(thread)

    def send_request(self, request, callback=None):
        '''Queues up a request to send to the connection.

        Inputs:
        request - A tornado.httpclient.HTTPRequest object
        callback - If it is a function, it will be called when the request
                   returns with its response as the parameter. If it is None,
                   this call blocks and returns the HTTPResponse.
        '''
        if request is None:
            raise TypeError('Request must be non Null')
        
        if callback is not None:
            self.request_q.put((request, callback, 0))
            return

        # Setup a callback that pushes the response into the queue and
        # then wait on that queue.
        response_q = Queue.Queue()
        self.request_q.put((request,
                            lambda response: response_q.put(response),
                            0))
        return response_q.get()

    def stop(self):
        '''Stops the connection pool.'''
        for thread in self.threads:
            thread.stop()

        for thread in self.threads:
            thread.join()

    def join(self):
        '''Blocks until all the requests have been processed.'''
        self.request_q.join()

                
