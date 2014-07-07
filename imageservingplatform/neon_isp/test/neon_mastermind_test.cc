/*
 * Test all the lookup methods in neon_mastermind.cpp
 *
 * Methods tested indirectly:
 *
 * */

#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>
#include <libgen.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>

#include "neon_mastermind.h"                                                         
#include "neon_log.h"                                                                
#include "neon_updater.h"
#include "neon_error_codes.h"

using namespace std;                                                                 

class NeonMastermindTest: public ::testing::Test {                                   
    public:                                                                              
        NeonMastermindTest() {
        }
    protected:
        virtual void SetUp(){
            boost::scoped_ptr<char> curPath(strdup(__FILE__));
            string curDir = dirname(curPath.get());
            string mastermind = curDir + "/mastermind";
            neon_mastermind_init();
            neon_mastermind_load(mastermind.c_str());                                                
        }
};

TEST_F(NeonMastermindTest, test_account_id_lookup){

    char *pid = "pub1";                                                              
    const char * aid = 0;                                                            
    int a_size;
    NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_ERROR err = neon_mastermind_account_id_lookup(
                                                    pid, &aid, &a_size);
    //assert(aid != 0);
    EXPECT_EQ(err, NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_OK);
} 

TEST_F(NeonMastermindTest, test_account_id_lookup_fail){
    char *pid = "invalid_pub"; 
    const char * aid = 0;
    int a_size;
    NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_ERROR err = neon_mastermind_account_id_lookup(
                                                    pid, &aid, &a_size);
    EXPECT_EQ(0, aid);
    EXPECT_EQ(err, NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_NOT_FOUND);
}

TEST_F(NeonMastermindTest, test_neon_mastermind_image_url_lookup){

    char *pid = "pub1";
    char *vid = "vid1";
    char *aid = "acc1";
    ngx_str_t ip = ngx_string("12.251.6.7");
    int h = 500;
    int w = 600;
    const char * url = 0;
    int size;

    NEON_MASTERMIND_IMAGE_URL_LOOKUP_ERROR err = neon_mastermind_image_url_lookup(
                                                    aid, vid, &ip, h, w, &url, &size);
    EXPECT_EQ(err, NEON_MASTERMIND_IMAGE_URL_LOOKUP_OK);
    EXPECT_STRNE(url, NULL);

    // Empty IP Address String
    ip = ngx_string("");
    err = neon_mastermind_image_url_lookup(aid, vid, &ip, h, w, &url, &size);
    EXPECT_EQ(err, NEON_MASTERMIND_IMAGE_URL_LOOKUP_OK);
    EXPECT_STRNE(url, NULL);

}

/*
 * Lookup failure test cases
 * */
TEST_F(NeonMastermindTest, test_neon_mastermind_image_url_lookup_invalids){

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
    NEON_MASTERMIND_IMAGE_URL_LOOKUP_ERROR err = neon_mastermind_image_url_lookup(
                                                    aid, vid, &ip, h, w, &url, &size);
    EXPECT_EQ(err, NEON_MASTERMIND_IMAGE_URL_LOOKUP_NOT_FOUND);
    aid[0] = 'a';

    // Invalid video id
    vid[0] = 'x';
    err = neon_mastermind_image_url_lookup(aid, vid, &ip, h, w, &url, &size);
    EXPECT_EQ(err, NEON_MASTERMIND_IMAGE_URL_LOOKUP_NOT_FOUND);
    vid[0] = 'v';

    // invalid height
    h = 1000;	
    err = neon_mastermind_image_url_lookup(aid, vid, &ip, h, w, &url, &size);
    EXPECT_EQ(err, NEON_MASTERMIND_IMAGE_URL_LOOKUP_OK);
    EXPECT_STRNE(url, NULL);
    h = 500;

    // invalid width
    w = 1000;
    err = neon_mastermind_image_url_lookup(aid, vid, &ip, h, w, &url, &size);
    EXPECT_EQ(err, NEON_MASTERMIND_IMAGE_URL_LOOKUP_OK);
    EXPECT_STRNE(url, NULL);
    w = 600;	

}

TEST_F(NeonMastermindTest, test_neon_mastermind_tid_lookup){

    char *pid = "pub1";
    char *vid = "vid1";
    char *aid = "acc1";
    ngx_str_t ip = ngx_string("12.251.6.7");
    const char * tid= 0;
    int size;

    NEON_MASTERMIND_TID_LOOKUP_ERROR err = neon_mastermind_tid_lookup(aid, vid, &ip, &tid, &size);
    EXPECT_EQ(err, NEON_MASTERMIND_TID_LOOKUP_OK);
    EXPECT_STRCASEEQ("thumb1", tid);
}

TEST_F(NeonMastermindTest, test_invalid_mastermind_load){
    boost::scoped_ptr<char> curPath(strdup(__FILE__));
    string curDir = dirname(curPath.get());
    string mastermind = curDir + "/mastermind.invalid";
    neon_mastermind_init();
    NEON_LOAD_ERROR ret = neon_mastermind_load(mastermind.c_str());
    EXPECT_EQ(ret, NEON_LOAD_FAIL);
}
