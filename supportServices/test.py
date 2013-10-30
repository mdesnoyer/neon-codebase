'''
Sample test script to create account, test brightcove and youtube integrations
'''
import urllib
import urllib2
import json
import time
import random

#TODO: UNIT TEST

#constants
BASE = 'http://localhost:8083/api/v1'
a_id = 'test12345'

#helper http methods

def put_request(url,params,key):
    data = urllib.urlencode(params)
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    headers = {'X-Neon-API-Key' : key }
    request = urllib2.Request(url, data, headers)
    request.get_method = lambda: 'PUT'
    resp = opener.open(request)
    return resp.read() 

def post_request(url,pparams,key):
    try:
        data = urllib.urlencode(pparams)
        headers = {'X-Neon-API-Key' : key }
        req = urllib2.Request(url, data, headers)
        response = urllib2.urlopen(req)
        return response.read()
    except Exception,e:
        print e

def get_request(url,key):
    headers = {'X-Neon-API-Key' : key }
    #response = urllib2.urlopen(url,data='',headers=headers)
    
    data = None 
    try:
        req = urllib2.Request(url,None,headers)
        response = urllib2.urlopen(req)
        data = response.read()
    except urllib2.HTTPError, error:
        print error.read()

    return data

def refresh_youtube_token(token):
    #test app
    vals = {'grant_type' :  'refresh_token', 'client_id' : '6194976668-0k91122985fi86ctfbmv7efculmjc3ih.apps.googleusercontent.com',
            'client_secret' : 'e1N4-XRmjCSXktVeRh638_U1', 'refresh_token' : token } 
    url = 'https://accounts.google.com/o/oauth2/token'
    data = urllib.urlencode(vals)
    req = urllib2.Request(url, data)
    try:
        response = urllib2.urlopen(req)
        print response.read()
    except urllib2.HTTPError, error:
        print error.read()


'''
User flows

Signups

create neon user --> use neon api directly
create neon user --> brightcove account signup  
create neon user --> brightcove account signup --> bcove-youtube channel  
create neon user --> youtube channel account --> use neon-yt api via UI

update brightcove tokens
add new brightcove integration/account
update youtube token
add new youtube integration

#Account Mgmt
get account status for any/all my integrations

#Video management
get videos for particular integration

#Youtube Flows
create youtube account, link it to brightcove
submit video processing request for a new video
update thumbnail for existing youtube video

-- for test use my test youtube account, force execution of client code to process the video
-- update thumbnail for a known video in the test account
'''

''' get status given a video '''
def get_video_status(a_id,i_id,key,vid,itype='neon_integrations'):
    url = BASE + '/accounts/' + a_id + '/' + itype + '/' + i_id + '/videos/' + vid 
    data = get_request(url,key)

''' query server for job status'''
def get_video_status(key,job_id):
    url = 'http://localhost:8081/api/v1/jobstatus?api_key=' + key + '&job_id=' + job_id
    data = get_request(url,key)
    return data

''' create neon account '''
def create_neon_account(a_id):
    vals = { 'account_id' : a_id }
    url = BASE + '/accounts'
    print "create account ",  url
    resp = post_request(url,vals,'')
    if not resp:
        print "Account not created"
        exit(0)
    key = json.loads(resp)['neon_api_key']
    return key

def create_youtube_account(a_id,i_id='y1234'):
    url = BASE + '/accounts/' + a_id + '/youtube_integrations'
    vals = {'refresh_token' :'1/KEsExNfa6E7Je357oeC9PuaMET2KGw7D2cpZqRkqLx4', 'access_token': 'init', 'expires': '0.1' ,'integration_id': i_id, 'auto_update': False } 
    resp = post_request(url,vals,key)
    return resp

def test_signup_flow(a_id,i_id='i12345'):
    
    #1. create account
    key = create_neon_account(a_id)

    #2. update account 
    url = BASE + '/accounts'
    url = url + '/' + a_id
    vals = {'processing_minutes' : 450 , 'plan_start_date' : time.time() }
    print "update account " , url
    resp = put_request(url,vals,key)

    #3. Create Brightcove account
    url = BASE + '/accounts/' + a_id + '/brightcove_integrations'
    vals = { 'integration_id' : i_id, 'publisher_id' : 'testpubid123' , 'read_token' : 'cLo_SzrziHEZixU-8hOxKslzxPtlt7ZLTN6RSA7B3aLZsXXF8ZfsmA..', 'write_token': 'vlBj_kvwXu7r1jPAzr8wYvNkHOU0ZydFKUDrm2fnmVuZyDqEGVfsEg..','auto_update': True }
    print "creating brightcove account", url
    resp = post_request(url,vals,key)
    print resp 

    #. Check for brightcove video status
    url = BASE + '/accounts/' + a_id + '/brightcove_integrations/' + i_id +"/videos/x?video_ids=2679202484001,2640728545001"
    print url
    data = get_request(url,key)
    print data
    return #test only bcove account
    
    #4. Update brightcove account
    url = BASE + '/accounts/' + a_id +'/brightcove_integrations/' + i_id 
    vals = { 'read_token' : 'cLo_SzrziHEZixU-8hOxKslzxPtlt7ZLTN6RSA7B3aLZsXXF8ZfsmA..', 'write_token': 'vlBj_kvwXu7r1jPAzr8wYvNkHOU0ZydFKUDrm2fnmVuZyDqEGVfsEg..','auto_update': False }
    resp = put_request(url,vals,key)
    print resp 


    #create youtube account
    create_youtube_account()

    url = BASE + '/accounts/' + a_id +'/brightcove_integrations/' + "wrongbcoveid" 
    vals = { 'read_token' : 'cLo_SzrziHEZixU-8hOxKslzxPtlt7ZLTN6RSA7B3aLZsXXF8ZfsmA..', 'write_token': 'vlBj_kvwXu7r1jPAzr8wYvNkHOU0ZydFKUDrm2fnmVuZyDqEGVfsEg..','auto_update': False }
    resp = put_request(url,vals,key)
    #expect HTTP 400

    url = BASE + '/accounts/brightcove_integrations/wrongbcoveid'

def videos(key,i_id):
    vid = '2296855886001'

    #. Get videos for account
    url = BASE + '/accounts/' + a_id + '/videos'
    print "get videos for account ", url
    print get_request(url,key)

    # Create bcove video api request
    url = BASE + '/accounts/' + a_id + '/brightcove_integrations/' + i_id + '/create_video_request'  
    vals = {'video_id' : 2296855886001 }
    resp = put_request(url,vals,key)
    print resp

    #Get brightcove videos
    url = BASE + '/accounts/' + a_id + '/brightcove_integrations/' + i_id +  '/videos'
    print "get videos for account ", url
    print get_request(url,key)
    
    #6. Get a particular video
    url = BASE + '/accounts/' + a_id + '/brightcove_integrations/' + i_id +  '/videos/' + vid
    print "get videos for account ", url
    print get_request(url,key)
    
    #7. Update particular video

    #### Youtube Stuff

    #Get videos from youtube account
    url = BASE + '/accounts/' + a_id + '/youtube_integrations/' + y_id + '/videos'
    print get_request(url,'c2bf18e51cdf29afdf0fe56e9e513825')

    #Create youtube video request
    url = BASE + '/accounts/' + a_id + '/youtube_integrations/y1234/create_video_request'  
    vals = {'video_id' : 'MvGovQxEqxw',  'video_url' : "http://www.youtube.com/watch?v=MvGovQxEqxw"  }
    resp = put_request(url,vals, 'c2bf18e51cdf29afdf0fe56e9e513825')
    print resp

    # Account status
    url = BASE + '/accounts/' + a_id + '/status'
    print "get account status", url
    print get_request(url,'853657360c62d5448f1cf69998f8cb86')

'''
 #Create request for test video, wait for it to complete processing then check if thumbnail changed
 #use job status api to check state of the request
'''
def youtube_test():

    a_id = "test_yt" + str(random.random())
    y_id = 'y1234'
    vid = 'MvGovQxEqxw'
    
    def youtube_test_cleanup(key):
        #delete account
        url = BASE + '/removeaccount/' + a_id
        get_request(url,key)

    #create neon account
    key = create_neon_account(a_id)

    #create yt account
    create_youtube_account(a_id,y_id)

    #create youtube request
    url = BASE + '/accounts' + a_id + '/youtube_integrations/' + y_id + '/create_video_request'
    vals = {'video_id' : vid,  'video_url' : "https://copy.com/pFoRsysJNJDD" }
    job_id = put_request(url,vals,key)

    #Query job id
    done = False
    while not done:
        jstatus = get_job_status(key,job_id)
        status = json.loads(jstatus)
        if status.state == 'finished':
            done = True
        
        if status.state == 'failed': 
            done = True
            print "[VIDEO ERROR]"
            youtube_test_cleanup(key)
            exit(0)

        time.sleep(30)

    #update thumbnail for a known video
    url = BASE + '/accounts' + a_id + '/youtube_integrations/' + y_id + '/videos/' + vid
    vals = {'': ''}
    print "Update thumbnail ", url 

    #Cleanup
    youtube_test_cleanup(key)


#############  TEST #############


test_signup_flow('test_new_account' +str(time.time()))

