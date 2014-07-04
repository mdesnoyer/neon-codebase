#include <time.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>

#include "neon_mastermind.h"
#include "neon_log.h"
#include "neon_updater.h"
#include "neon_error_codes.h"

//// Tests /////

void
test_load_invalid_mastermind(){

	//Invalid path
	char * mastermind = "invalid_path"; 
    NEON_LOAD_ERROR err = neon_mastermind_load(mastermind);
    assert(err == NEON_LOAD_FAIL);
}

void 
test_account_id_lookup(){

	char *pid = "pub1"; 
    const char * aid = 0;
	int a_size;
	NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_ERROR err = neon_mastermind_account_id_lookup(pid, &aid, &a_size);
	assert(aid != 0);
	assert(err == NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_OK); 
	//printf("Account ID lookup success\n");

}

void
test_account_id_lookup_fail(){
	char *pid = "invalid_pub"; 
    const char * aid = 0;
	int a_size;
	NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_ERROR err = neon_mastermind_account_id_lookup(pid, &aid, &a_size);
	assert(aid == 0);
	assert(err == NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_NOT_FOUND);
}

void
test_neon_mastermind_image_url_lookup(){

	char *pid = "pub1";
	char *vid = "vid1";
	char *aid = "acc1";
	ngx_str_t ip = ngx_string("12.251.6.7");
	int h = 500;
	int w = 600;
	const char * url = 0;
	int size;

	NEON_MASTERMIND_IMAGE_URL_LOOKUP_ERROR err = neon_mastermind_image_url_lookup(aid, vid, &ip, h, w, &url, &size);
	assert(err == NEON_MASTERMIND_IMAGE_URL_LOOKUP_OK);
	assert(url != 0);
	//printf("URL %s \n", url);
	
	// Empty IP Address String
	ip = ngx_string("");
	err = neon_mastermind_image_url_lookup(aid, vid, &ip, h, w, &url, &size);
	assert(err == NEON_MASTERMIND_IMAGE_URL_LOOKUP_OK);
	assert(url != 0);
	
}

/*
 * Lookup failure test cases
 * */
void
test_neon_mastermind_image_url_lookup_invalids(){

	char *pid = "pub1";
	char vid[] = "vid1";
	char aid[] = "acc1";
	ngx_str_t ip = ngx_string("12.251.6.7");
	int h = 500;
	int w = 600;
	const char * url = 0;
	int size;

	// invalid account id
	aid[0] = 'i';
	NEON_MASTERMIND_IMAGE_URL_LOOKUP_ERROR err = neon_mastermind_image_url_lookup(aid, vid, &ip, h, w, &url, &size);
	assert(err == NEON_MASTERMIND_IMAGE_URL_LOOKUP_NOT_FOUND);
	aid[0] = 'a';

	// Invalid video id
	vid[0] = 'x';
	err = neon_mastermind_image_url_lookup(aid, vid, &ip, h, w, &url, &size);
	assert(err == NEON_MASTERMIND_IMAGE_URL_LOOKUP_NOT_FOUND);
	vid[0] = 'v';

	// invalid height
	h = 1000;	
	err = neon_mastermind_image_url_lookup(aid, vid, &ip, h, w, &url, &size);
	assert(err == NEON_MASTERMIND_IMAGE_URL_LOOKUP_OK);
	assert(url != 0); //verify default URL
	h = 500;

	// invalid width
	w = 1000;
	err = neon_mastermind_image_url_lookup(aid, vid, &ip, h, w, &url, &size);
	assert(err == NEON_MASTERMIND_IMAGE_URL_LOOKUP_OK);
	assert(url != 0); //verify default URL
	w = 600;	

}

void
test_neon_mastermind_tid_lookup(){

	char *pid = "pub1";
	char *vid = "vid1";
	char *aid = "acc1";
	ngx_str_t ip = ngx_string("12.251.6.7");
	const char * tid= 0;
	int size;

	NEON_MASTERMIND_TID_LOOKUP_ERROR err = neon_mastermind_tid_lookup(aid, vid, &ip, &tid, &size);
	assert(err == NEON_MASTERMIND_TID_LOOKUP_OK);
	assert(strcmp("thumb1", tid) == 0);
}


int
main(int argc, char ** argv)
{
    
   	// Setup 
	
    int loops = 0;
    char * mastermind = "test/mastermind";
    
    if(argc == 1) {
        loops = 1;
    }
    else {
        loops = atoi(argv[1]);
        mastermind = argv[2];
    }
        
    NEON_BOOLEAN b = neon_mastermind_init();
    
    int i=0;
    
    for(; i < loops; i++) {
    
        NEON_LOAD_ERROR err = neon_mastermind_load(mastermind);
        assert(err == NEON_LOAD_OK);
    }

	// Tests
	test_account_id_lookup();	
	test_account_id_lookup_fail();   
	test_neon_mastermind_image_url_lookup();
	test_neon_mastermind_image_url_lookup_invalids();
	
	test_load_invalid_mastermind();
	
	// Run tests again, attempting to load invalid mastermind should be
	// handled and old mastermind data should continue to persist
	test_account_id_lookup();	
	test_neon_mastermind_image_url_lookup();
	test_neon_mastermind_image_url_lookup_invalids();
	test_neon_mastermind_tid_lookup();
}



