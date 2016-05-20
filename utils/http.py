'''A module full of http tools

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''
import os.path
import sys
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

import concurrent.futures
import logging
import multiprocessing
import random
import socket
import threading
import time
import tornado.escape
import tornado.gen
import tornado.httpclient
import tornado.ioloop
import tornado.locks
import urlparse
import utils.logs
from utils import statemon
import utils.sync

_log = logging.getLogger(__name__)

statemon.state.define('waiting_in_pools', int)
_waiting_in_pools_ref = statemon.state.get_ref('waiting_in_pools')


class ResponseCode(object):
    HTTP_OK = 200
    HTTP_ACCEPTED = 202
    HTTP_BAD_REQUEST = 400
    HTTP_UNAUTHORIZED = 401
    HTTP_PAYMENT_REQUIRED = 402
    HTTP_FORBIDDEN = 403
    HTTP_NOT_FOUND = 404
    HTTP_CONFLICT = 409
    HTTP_TOO_MANY_429 = 429
    HTTP_INTERNAL_SERVER_ERROR = 500
    HTTP_NOT_IMPLEMENTED = 501


class HTTPVerbs(object):
    POST = 'POST'
    PUT = 'PUT'
    GET = 'GET'
    DELETE = 'DELETE'
    PATCH = 'PATCH'


# TODO(mdesnoyer): Handle the stack on async requests so that the
# callback will have a stack that looks like the original request
# being called.
@utils.sync.optional_sync
@tornado.gen.coroutine
def send_request(request, ntries=5, do_logging=True, base_delay=0.2,
                 no_retry_codes=None, retry_forever_codes=None):
    '''Sends an HTTP request with retries

    If there was an error, either in the connection, or if the
    response is json and has a non-nill "error" field, the
    response.error will be a tornado.httpclient.HTTPError

    The retries occur with exponential backoff

    request - A tornado.httpclient.HTTPRequest object
    ntries - Number of times to try sending the request
    do_logging - True if logging should be turned on
    base_delay - Time in seconds for the first delay on the retry
    no_retry_codes - List of http codes that cause the request not to retry
    retry_forever_codes - List of http status code that cause request to retry
        forever until success (e.g., 429) with backoff
    '''
    # Verify the request url
    parsed = urlparse.urlsplit(unicode(request.url))
    if parsed.scheme not in ("http", "https"):
        msg = ('Invalid url to request because the scheme is %s: %s' %
               (parsed.scheme, request.url))
        if do_logging:
            _log.error(msg)
        raise tornado.gen.Return(tornado.httpclient.HTTPResponse(
            request, 400, error=tornado.httpclient.HTTPError(400, msg)))

    no_retry_codes = no_retry_codes or []
    retry_forever_codes = retry_forever_codes or []

    cur_try = 0
    response = None
    while cur_try < ntries or (response and response.error and
                               response.error.code in retry_forever_codes):
        cur_try += 1
        try:
            http_client = tornado.httpclient.AsyncHTTPClient()
            response = yield http_client.fetch(request)
        except tornado.httpclient.HTTPError as e:
            if e.response:
                response = e.response
            else:
                response = tornado.httpclient.HTTPResponse(request,
                                                           e.code,
                                                           error=e)
        except socket.error as e:
            # Socket resolution error
            if do_logging:
                _log.error('Socket resolution error for %r' % request.url)
            error = tornado.httpclient.HTTPError(
                502, 'socket error')
            response = tornado.httpclient.HTTPResponse(request,
                                                       502,
                                                       error=error)
        if not response.error:
            try:
                data = tornado.escape.json_decode(response.body)
                if isinstance(data, dict) and data['error']:
                    if do_logging:
                        _log.warning_n(('Response error from %s: %s') %
                                       (request.url, data['error']), 5)
                    response.error = tornado.httpclient.HTTPError(
                        500, str(data['error']))
                else:
                    raise tornado.gen.Return(response)

                                
            except ValueError:
                # It's not JSON data so we're done
                raise tornado.gen.Return(response)

            except KeyError:
                # The JSON doens't have an error field, so
                # we're done
                raise tornado.gen.Return(response)

        elif response.error.code in no_retry_codes:
            # We received a response that says we shouldn't retry, so
            # just return the response.
            raise tornado.gen.Return(response)
            
        else:
            if do_logging:
                _log.warn_n(('Error sending request to %s: %s') %
                             (request.url, response.error),
                             5)


        delay = (1 << cur_try) * base_delay * random.random() # in seconds
        yield tornado.gen.sleep(delay)

    if do_logging:
        _log.warn_n('Too many errors connecting to %s' % request.url,
                    3)
    raise tornado.gen.Return(response)

class RequestPool(object):
    '''Handles a number of concurrent requests to an http service.

    Ensures that target isn't hit too hard too quickly.
    Includes exponential retries.
    '''
    def __init__(self, max_connections=1, limit_for_subprocs=False,
                 thread_safe=False):
        '''Creates the request pool.

        Inputs:
        max_connections - Maximum connections allowed
        limit_for_subprocs - If True then we limit the connections for 
                             all subprocesses to. The default is to have 
                             max_connections per process, which makes 
                             the synchronization much more efficient if you 
                             don't need strict limits. 
        thread_safe - If False, send_request can only be submitted from a
                      single thread. It will be more efficient though.
        '''
        if limit_for_subprocs:
            self._lock = utils.sync.FutureLock(
                multiprocessing.BoundedSemaphore(max_connections))
        elif thread_safe:
            self._lock = utils.sync.FutureLock(
                threading.BoundedSemaphore(max_connections))
        else:
            self._lock = tornado.locks.BoundedSemaphore(max_connections)

    @utils.sync.optional_sync
    @tornado.gen.coroutine
    def send_request(self, request, **kwargs):
        '''Sends a request to the pool.

        Acts exactly like the module level send_request

        Inputs:
        request - A tornado.httpclient.HTTPRequest object
        kwargs - Passed to the module level send_request
        '''
        statemon.state.increment(ref=_waiting_in_pools_ref)
        yield self._lock.acquire()
        try:
            kwargs['async'] = True
            response = yield send_request(request, **kwargs)
            raise tornado.gen.Return(response)
        finally:
            self._lock.release()
            statemon.state.increment(ref=_waiting_in_pools_ref, diff=-1)

                
