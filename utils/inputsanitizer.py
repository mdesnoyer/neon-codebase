'''
Input sanitizer module to clean up _inputs and convert to particular datatype
'''

import datetime
import dateutil.parser
import logging
import json
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

    @staticmethod
    def html_safe(_input):
        return _input.encode('ascii', 'xmlcharrefreplace')

    @staticmethod
    def to_list(_input):
        if isinstance(_input, basestring):
           pass

    @staticmethod
    def to_dict(_input):
        if isinstance(_input, dict):
            return _input
        return json.loads(_input)

    @staticmethod
    def to_bool(_input, is_null_valid=False):

        if isinstance(_input, bool):
            return _input

        if _input is None:
            if is_null_valid:
                return None
            raise Exception("Missing _input or None")
        
        if isinstance(_input, basestring):
            if _input.lower() == 'false':
               return False 
            if _input.lower() == 'true':
               return True

        else:
            raise Exception("Conversion not supported")

    @staticmethod
    def to_string(_input):
        if _input is None:
            return None
        
        elif isinstance(_input, basestring):
            return _input

        elif isinstance(_input, list): 
           return "".join(_input)
       
        elif isinstance(_input, map): 
           return "".join((map(str, _input)))
       
        else:
           raise Exception("Conversion not supported")

    @staticmethod
    def to_alphanumeric(_input):
        OK_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        out = ''
        if isinstance(_input, basestring):
            for x in _input:
                if x in OK_CHARS:
                    out += x
            return out
        else:
           raise Exception("Conversion not supported")

    @staticmethod
    def to_urlchars(_input):
        OK_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789 .,!?:"
        out = ''
        for x in _input:
            if x in OK_CHARS:
                out += x
        return out

    @staticmethod
    def to_no_unicode(_input):
        return _input.encode('punycode')

    @staticmethod
    def validate_http_url(_input):
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

    @staticmethod
    def sanitize_null(ip):

        '''
        Sanitize null or undefined strings from tracker to python None
        '''
        if ip == "null" or ip == "undefined" or ip == "":
            return None
        return ip

    @staticmethod
    def sanitize_string(ip):
        if ip == "null" or ip == "undefined" or ip == "" or ip is None:
            return None
        return unicode(ip)

    @staticmethod
    def sanitize_int(ip):
        '''
        Sanitize null or undefined strings from tracker data to INT or None
        '''
        if ip is None or ip == "null" or ip == "undefined":
            return None
        return int(ip)

    @staticmethod
    def sanitize_float(fl):
        '''Sanitize null or undefined strings to float or None.'''
        if fl is None or fl == 'null' or fl == 'undefined':
            return None
        return float(fl)

    @staticmethod
    def sanitize_date(ip):
        if ip is None or ip == "null" or ip == "undefined":
            return None
        try:
            epoch = float(ip)
            return datetime.datetime.utcfromtimestamp(epoch)
        except ValueError:
            return dateutil.parser.parse(ip)
        return None

