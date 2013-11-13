'''A module that contains thread pools of connections to outside services.

Used to handle throttling or synchronizing connections to those services.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import logging
import Queue
import threading
import time
import tornado.escape
import tornado.httpclient
import tornado.ioloop

_log = logging.getLogger(__name__)

class HttpConnectionThread(threading.Thread):
    def __init__(self, q, max_retries):
        super(HttpConnectionThread, self).__init__()
        self.q = q
        self.max_retries = max_retries
        self.daemon = True
        self._stopped = threading.Event()

    def stop(self):
        self._stopped.set()
        self.q.put((None, None, None))

    def run(self):
        while not self._stopped.is_set():
            try:
                request, callback, n_tries = self.q.get()

                if request is None:
                    continue

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
                    
                n_tries += 1
                if not response.error:
                    try:
                        data = tornado.escape.json_decode(response.body)
                        if data['error']:
                            _log.warning(
                                ('key=http_response_error '
                                 'msg=Response err from %s: %s') %
                                 (request.url, data['error']))
                        else:
                            callback(response)
                            continue

                                
                    except ValueError:
                        # It's not JSON data so just call the callback
                        callback(response)
                        continue

                    except KeyError:
                        # The JSON doens't have an error field, so
                        # call the callback
                        callback(response)
                        continue

                else:
                    _log.warning(('key=http_connection_error '
                                  'msg=Error connecting to %s: %s') %
                                  (request.url, response.error))

                if n_tries >= self.max_retries:
                    _log.error(('key=http_too_many_errors '
                                'msg=Abort. Too many errors for request: %s')
                                % request)
                    if response.error is None:
                        response.error = tornado.httpclient.HTTPError(
                            503, 'Too many errors connecting to %s' 
                            % request.url)
                    callback(response)
                else:
                    # Requeue the request after a delay
                    delay = (1 << n_tries) * 0.1 # in seconds
                    io_loop = tornado.ioloop.IOLoop.instance()
                    io_loop.add_callback(
                        lambda: io_loop.add_timeout(
                            time.time() + delay,
                            lambda: self.q.put((request, callback, n_tries)))) 

            except Exception as e:
                _log.exception(
                    'key=http_connection msg=Unhandled exception: %s'
                    % e)            

class HttpConnectionPool(object):
    '''Handles a number of concurrent requests to an http service.

    Ensures that target isn't hit too hard too quickly.
    Includes exponential retries.
    '''
    def __init__(self, max_connections=1, max_retries=5):
        self.request_q = Queue.Queue()

        self.threads = []
        for i in range(max_connections):
            thread = HttpConnectionThread(self.request_q, max_retries)
            thread.start()
            self.threads.append(thread)

    def send_request(self, request, callback=None):
        '''Queues up a request to send to brightcove.

        Inputs:
        request - A tornado.httpclient.HTTPRequest object
        callback - If it is a function, it will be called when the request
                   returns with its response as the parameter. If it is None,
                   this call blocks and returns the HTTPResponse.
        '''
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
