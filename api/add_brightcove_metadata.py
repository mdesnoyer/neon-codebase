
#!/usr/bin/python
import tornado.escape
import sys
import os
import hashlib


fname = "brightcoveCustomerTokens.json"

class BrightcoveMetadata(object):

    def __init__(self,publisher_name,read_token,write_token,neon_api_key,publisher_id):
        self.publisher_name = publisher_name
        self.read_token = read_token
        self.write_token = write_token
        self.neon_api_key = neon_api_key
        self.publisher_id = publisher_id

    def save(self):
        json_data = '{}'
        if os.path.exists(fname):
            with open(fname, 'r') as f:
                json_data = f.readline()

        data = tornado.escape.json_decode(json_data) 
        data[self.publisher_name] = {} 
        data[self.publisher_name]['read_token']  =  self.read_token
        data[self.publisher_name]['write_token'] =  self.write_token
        data[self.publisher_name]['neon_api_key'] = self.neon_api_key
        data[self.publisher_name]['publisher_id'] = self.publisher_id
        json = tornado.escape.json_encode(data)

        with open(fname,'w') as f:
            f.write(json)


publisher_name = sys.argv[1]
read_token = sys.argv[2]
write_token = sys.argv[3]
neon_api_key = sys.argv[4]
publisher_id = sys.argv[5]

bm = BrightcoveMetadata(publisher_name,read_token,write_token,neon_api_key,publisher_id)
bm.save()
