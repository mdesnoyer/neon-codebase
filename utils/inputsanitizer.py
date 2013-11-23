'''
Input sanitizer module to clean up inputs and convert to particular datatype
'''

import logging
import tornado.escape
import tornado.httpclient

_log = logging.getLogger(__name__)

class InputSanitizer(object):
    def __init__(self):
        pass

    @classmethod
    def html_safe(cls,input):
        return input.encode('ascii', 'xmlcharrefreplace')

    @classmethod
    def to_list(cls,input):
        if isinstance(input,basestring):
           pass

    @classmethod
    def to_bool(cls,input):

        if input is None:
            raise Exception("Missing input or None")
        
        if isinstance(input,basestring):
            if input.lower() == 'false':
               return False 
            if input.lower() == 'true':
               return True
        
        elif isinstance(input,bool):
            return input

        else:
            raise Exception("Conversion not supported")

    @classmethod
    def to_string(cls,input):
        if isinstance(input,basestring):
            return input

        elif isinstance(input,list): 
           return "".join(input)
       
        elif isinstance(input,map): 
           return "".join((map(str,input)))
       
        else:
           raise Exception("Conversion not supported")

    @classmethod
    def to_alphanumeric(cls,input):
        OK_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        out = ''
        if isinstance(input,basestring):
            for x in input:
                if x in OK_CHARS:
                    out += x
            return out
        else:
           raise Exception("Conversion not supported")

    @classmethod
    def to_urlchars(cls,input):
        OK_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789 .,!?:"
        out = ''
        for x in input:
            if x in OK_CHARS:
                out += x
        return out

