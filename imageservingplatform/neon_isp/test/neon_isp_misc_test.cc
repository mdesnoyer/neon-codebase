/*
 * Misc testing (Test methods from neon_utils, updater..)
 *
*/

#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>
#include <libgen.h>
#include <stdio.h>
#include <time.h>

#include "neon_error_codes.h"
extern "C" {
    #include "neon_updater.h"
    #include "neon_utc.h"
    #include "neon_utils.h"
}
using namespace std;                                                                 

class NeonISPMiscTest: public :: testing::Test{

public:
        NeonISPMiscTest(){}
protected:
        virtual void SetUp(){
            boost::scoped_ptr<char> curPath(strdup(__FILE__));
            curDir = dirname(curPath.get());
        }
        string curDir;
};

/*
TEST_F(NeonISPMiscTest, test_run_loop){ 
    void * ret = neon_runloop(NULL);
    EXPECT_EQ(0, 0); // how do you test this ?
   }
*/

TEST_F(NeonISPMiscTest, DISABLED_test_utc_tester){
        const char * str = "2014-01-02T01:02:03Z";
        time_t correct = 1388649723; //1388624523;
        
        time_t result = 0;
        int ret = neon_convert_string_to_time(str, &result);
        
        EXPECT_EQ(ret, NEON_UTC_OK);
        EXPECT_EQ(correct, result);
}


TEST_F(NeonISPMiscTest, DISABLED_test_utils_get_expiry){
        time_t correct = 1403065673; //1403040473;
        string fpath = curDir + "/mastermind.test"; 
        time_t result = neon_get_expiry(fpath.c_str());
        EXPECT_EQ(correct, result);
}
    
TEST_F(NeonISPMiscTest, test_uuid_generator){

    const int len = 10;
    char uuid[len];
    neon_get_uuid((char*)uuid, len);
    EXPECT_STRNE(uuid, NULL);
}

TEST_F(NeonISPMiscTest, test_ip_string){

    unsigned char *valid_ip = (unsigned char*)"12.12.12.12";
    NEON_BOOLEAN ret = neon_is_valid_ip_string(valid_ip);
    EXPECT_EQ(ret, NEON_TRUE);
}

TEST_F(NeonISPMiscTest, test_invalid_ip_string){
    const int sz = 4;
    unsigned char *invalid_ips[sz] = { (unsigned char *)"12.12", 
                                       (unsigned char *)"12.03.04.90", 
                                       (unsigned char *)"1..90", 
                                       (unsigned char *)"12.12.12.12.12"};
    NEON_BOOLEAN ret;
    for (int i=0; i<sz; i++){
        ret = neon_is_valid_ip_string(invalid_ips[i]);
        EXPECT_EQ(ret, NEON_FALSE);
    }
}


TEST_F(NeonISPMiscTest, DISABLED_test_utils_file_exist){
        string fpath = curDir + "/mastermind.validated.test"; 
        int ret = neon_check_file_exist(fpath.c_str());
        EXPECT_EQ(ret, NEON_TRUE);
       
        fpath = curDir + "/mastermind.invalid.test";
        ret = neon_check_file_exist(fpath.c_str());
        EXPECT_EQ(ret, NEON_TRUE);
        
}
