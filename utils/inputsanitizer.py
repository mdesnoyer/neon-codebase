'''
Input sanitizer module to clean up _inputs and convert to particular datatype
'''

import logging
import re
import tornado.escape
import tornado.httpclient

_log = logging.getLogger(__name__)

class InputSanitizer(object):
    '''
    Sanitize inputs. String helper class
    '''
    def __init__(self):
        pass

    @classmethod
    def html_safe(cls, _input):
        return _input.encode('ascii', 'xmlcharrefreplace')

    @classmethod
    def to_list(cls, _input):
        if isinstance(_input, basestring):
           pass

    @classmethod
    def to_bool(cls, _input):

        if isinstance(_input, bool):
            return _input

        if _input is None:
            raise Exception("Missing _input or None")
        
        if isinstance(_input, basestring):
            if _input.lower() == 'false':
               return False 
            if _input.lower() == 'true':
               return True

        else:
            raise Exception("Conversion not supported")

    @classmethod
    def to_string(cls, _input):
        if isinstance(_input, basestring):
            return _input

        elif isinstance(_input, list): 
           return "".join(_input)
       
        elif isinstance(_input, map): 
           return "".join((map(str, _input)))
       
        else:
           raise Exception("Conversion not supported")

    @classmethod
    def to_alphanumeric(cls, _input):
        OK_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        out = ''
        if isinstance(_input, basestring):
            for x in _input:
                if x in OK_CHARS:
                    out += x
            return out
        else:
           raise Exception("Conversion not supported")

    @classmethod
    def to_urlchars(cls, _input):
        OK_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789 .,!?:"
        out = ''
        for x in _input:
            if x in OK_CHARS:
                out += x
        return out

    @classmethod
    def to_no_unicode(cls, _input):
        return _input.encode('punycode')

    @classmethod
    def validate_http_url(cls, _input):
        #TO BE FIXED, Dont' use
 
        '''
        Check if _input is a valid http url 
        Copied from DJANGO url validator
        '''
        regex = re.compile(
            r'^(?:http|ftp)s?://' # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
            r'localhost|' #localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
            r'(?::\d+)?' # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        return regex.match(_input)

    @classmethod
    def sanitize_null(cls, ip):

        '''
        Sanitize null or undefined strings from tracker to python None
        '''
        if ip == "null" or ip == "undefined" or ip == "":
            return None
        return ip

    @classmethod
    def sanitize_int(cls, ip):
        '''
        Sanitize null or undefined strings from tracker data to INT or None
        '''
        if ip == "null" or ip == "undefined":
            return
        return int(ip)

