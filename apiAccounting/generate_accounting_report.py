#!/usr/bin/python
import os 
import tornado.escape
from datetime import datetime

try:
    #base_directory = sys.argv[1]
    base_directory = '/Users/sunilmallya/s3download'
except:
    print "./generate <directory>"
    exit(0)


customer_accounts = ['1a1887842e4da19de2980538b1ae72d4']  #os.listdir(base_directory)
metadata_fname = "video_metadata.txt" 
status_fname   = "status.txt"
response_fname  = "response.txt"
request_fname  = "request.txt"

job_status = { 'submitted' : [] , 'processing': [], 'requeued': [], 'completed' : [], 'error' : [] } 
counters = { 'duration': [] , 'bitrate': [], 'video_valence' :  [] } 
# jobs submitted
# jobs processing
# jobs requeued
# jobs completed
# video stats ( avg duration, bitrate, video_scores)

''' return json '''
def read_file_contents(fname):
    data = None
    if os.path.exists(fname):
        with open(fname) as f:
            contents = f.readline()
            data = tornado.escape.json_decode(contents)
    return data

for account in customer_accounts:

    if ".DS_Store" in account:
        continue

    #get all the job ids
    job_ids = os.listdir( base_directory + "/" + account)

    for job_id in job_ids:
        if ".DS_Store" in job_id:
            continue

        # request data
        fname = base_directory + "/" + account + "/" + job_id + "/" + request_fname
        request_data = read_file_contents(fname)

        # response data
        fname = base_directory + "/" + account + "/" + job_id + "/" + response_fname
        response_data  = read_file_contents(fname)

        # extract status
        fname = base_directory + "/" + account + "/" + job_id + "/" + status_fname
        status_data  = read_file_contents(fname)
        if status_data is not None:
            state = status_data["status"]["state"]
            # if response.txt exists and doesn't have error then job was completed
            if response_data is not None and response_data.has_key("error") and  response_data["error"] == "":
                state = "completed" 
            job_status[state].append(job_id)

        # extract data from metadata filename
        fname = base_directory + "/" + account + "/" + job_id + "/" + metadata_fname
        metadata = read_file_contents(fname)
        if metadata is not None:
            for t in counters.keys():
                counters[t].append(float(metadata[t])) 
        else:
            # del from processing state
            job_status[state].remove(job_id)
            job_status['error'].append(job_id)

        try:
            s = '%s,%s,%s' %(request_data['video_id'], datetime.fromtimestamp(float(metadata['end_time'])), metadata['duration']) 
            print s
        except:
            pass
print counters
print job_status
