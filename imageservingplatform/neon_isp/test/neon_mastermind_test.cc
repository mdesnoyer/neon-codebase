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

        void set_absolute_path(const char *fname, string *absFname){
            boost::scoped_ptr<char> curPath(strdup(__FILE__));
            string curDir = dirname(curPath.get());
            *absFname = curDir + fname;
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

// Base methods to test neon_mastermind 
// They are defined here so that they can be referenced
// And called multiple times
// TODO: Figure out an easier way to do with GTest

NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_ERROR 
_lookup_account_id(){
    char *pid = "pub1";                                                              
    const char * aid = 0;                                                            
    int a_size;
    NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_ERROR err = neon_mastermind_account_id_lookup(
                                                    pid, &aid, &a_size);
    return err;
}

void 
verify_neon_mastermind_tid_lookup(char * vid, char * expectedTid){

    //char *pid = "pub1";
    char *aid = "acc1";
    ngx_str_t bucketId = ngx_string("12");
    std::string tid(""); 

    neon_mastermind_tid_lookup(aid, vid, &bucketId, tid);
    EXPECT_STRCASEEQ(expectedTid, tid.c_str());
}

void _test_account_id_lookup_fail(){

    char *pid = "invalid_pub"; 
    const char * aid = 0;
    int a_size;
    NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_ERROR err = neon_mastermind_account_id_lookup(
                                                    pid, &aid, &a_size);
    EXPECT_EQ(0, aid);
    EXPECT_EQ(err, NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_NOT_FOUND);
}


////// Neon Mastermind Tests  /////  

TEST_F(NeonMastermindTest, test_account_id_lookup){

    NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_ERROR err = _lookup_account_id();
    EXPECT_EQ(err, NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_OK);
} 

TEST_F(NeonMastermindTest, test_account_id_lookup_fail){
    _test_account_id_lookup_fail();
}

TEST_F(NeonMastermindTest, test_neon_mastermind_image_url_lookup)
{
    char *vid = "vid1";
    char *aid = "acc1";
    ngx_str_t bucketId = ngx_string("12");
    int h = 500;
    int w = 600;

    //boost::scoped_ptr<std::string> scoped_url;  
    std::string image_url(""); 
    neon_mastermind_image_url_lookup(aid, vid, &bucketId, h, w, image_url);
    EXPECT_STRNE(image_url.c_str(), "");

    // Empty bucketId String
    bucketId = ngx_string("");
    neon_mastermind_image_url_lookup(aid, vid, &bucketId, h, w, image_url);
    EXPECT_STREQ(image_url.c_str(), "http://neon/thumb1_500_600.jpg"); // majority thumbnail 
    
    // no width & height 
    h = -1; w = -1;
    neon_mastermind_image_url_lookup(aid, vid, &bucketId, h, w, image_url);
    EXPECT_STREQ(image_url.c_str(), "http://default_image_url.jpg"); // default URL
    w = 600;

    // Approx height & width
    int heights[4] = {498, 499, 501, 502};
    for (int i=0; i < 4; i ++){
        h = heights[i];
        neon_mastermind_image_url_lookup(aid, vid, &bucketId, h, w, image_url);
        EXPECT_STREQ(image_url.c_str(), "http://neon/thumb1_500_600.jpg"); // majority thumbnail 
    }

}

/*
 * Lookup failure test cases
 *
 * */

TEST_F(NeonMastermindTest, test_neon_mastermind_image_url_lookup_invalids){

    char *pid = "pub1";
    char vid[] = "vid1";
    char aid[] = "acc1";
    //ngx_str_t ip = ngx_string("12.251.6.7");
    ngx_str_t bucketId = ngx_string("12");
    int h = 500;
    int w = 600;
    std::string image_url(""); 

    // invalid account id
    aid[0] = 'i';
    neon_mastermind_image_url_lookup(aid, vid, &bucketId, h, w, image_url);
    EXPECT_STREQ("",image_url.c_str());
    aid[0] = 'a';

    // Invalid video id
    vid[0] = 'x';
    neon_mastermind_image_url_lookup(aid, vid, &bucketId, h, w, image_url);
    EXPECT_STREQ("",image_url.c_str());
    vid[0] = 'v';
}


TEST_F(NeonMastermindTest, testverify_neon_mastermind_tid_lookup){

    char * vid = "vid1";
    char * expTid= "thumb1";
    verify_neon_mastermind_tid_lookup(vid, expTid);
}

// Test loading of an invalid mastermind & ensure that it
// fails to load and the current mastermind data structure is 
// unaffected

TEST_F(NeonMastermindTest, test_invalid_mastermind_load){

    string mastermind;
    set_absolute_path("/mastermind.invalid", &mastermind);
    NEON_LOAD_ERROR ret = neon_mastermind_load(mastermind.c_str());
    EXPECT_EQ(ret, NEON_LOAD_FAIL);

    // Run tests again, attempting to load invalid mastermind should be
    // handled and old mastermind data should continue to persist
    
    NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_ERROR err = _lookup_account_id();
    EXPECT_EQ(err, NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_OK);
    
    char * vid = "vid1";
    char * expTid = "thumb1";
    verify_neon_mastermind_tid_lookup(vid, expTid);
}

// Test loading the new mastermind again

TEST_F(NeonMastermindTest, test_new_mastermind_load){

    string mastermind;
    set_absolute_path("/mastermind.new", &mastermind);
    NEON_LOAD_ERROR ret = neon_mastermind_load(mastermind.c_str());
    EXPECT_EQ(ret, NEON_LOAD_OK);
    
    // Execute account lookup & tid lookups after new mastermind load
    // Ensure that the new mastermind file is loaded and reflected 
    
    NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_ERROR err = _lookup_account_id();
    EXPECT_EQ(err, NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_OK);

    _test_account_id_lookup_fail();

    char * vid = "vidn1";
    char * expTid = "thumb1";
    verify_neon_mastermind_tid_lookup(vid, expTid);
}

// Test loading an expired mastermind with an old expiry
TEST_F(NeonMastermindTest, test_loading_expired_mastermind){

    string mastermind;
    set_absolute_path("/mastermind.expired", &mastermind);
    NEON_LOAD_ERROR ret = neon_mastermind_load(mastermind.c_str());
    EXPECT_EQ(ret, NEON_LOAD_FAIL);
    
}

