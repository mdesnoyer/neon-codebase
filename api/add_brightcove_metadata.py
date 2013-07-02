
#!/usr/bin/python
import tornado.escape
import sys
import os
import hashlib

publisher_name = sys.argv[1]
read_token = sys.argv[2]
write_token = sys.argv[3]
neon_api_key = sys.argv[4]
publisher_id = sys.argv[5]

fname = "brightcoveCustomerTokens.json"
json_data = '{}'
if os.path.exists(fname):
    with open(fname, 'r') as f:
        json_data = f.readline()

data = tornado.escape.json_decode(json_data) 
data[publisher_name] = {} 
data[publisher_name]['read_token'] = read_token
data[publisher_name]['write_token'] = write_token
data[publisher_name]['neon_api_key'] = neon_api_key
data[publisher_name]['publisher_id'] = publisher_id
json = tornado.escape.json_encode(data)

with open(fname,'w') as f:
    f.write(json)

