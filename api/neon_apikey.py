#use: ./script customer_name

#!/usr/bin/python
import tornado.escape
import sys
import hashlib
import os

fname = "apikeys.json"
path = os.getcwd()

class APIKey(object):
    ''' API Key management class '''
    def __init__(self):
        self.json_data = '{}'
        if os.path.exists(fname):
            with open(fname, 'r') as f:
                self.json_data = f.readline()
        self.data = tornado.escape.json_decode(self.json_data) 
    
    def add_key(self,name):
        key = hashlib.md5(name).hexdigest()
        self.data[name] = key
        json = tornado.escape.json_encode(self.data)

        with open(fname,'w') as f:
            f.write(json)
   
        return key

    def delete_key(self,key):
        if key in self.data.keys():
            self.data.pop(key) 

        json = tornado.escape.json_encode(self.data)
        with open(fname,'w') as f:
            f.write(json)

if __name__ == "__main__":
    name = sys.argv[1]
    ak = APIKey()
    ak.add_key(name)

