/*
 * Misc testing
 *
*/

#include <gtest/gtest.h>
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
        
        }
};

/*
TEST_F(NeonISPMiscTest, test_run_loop){ 
    void * ret = neon_runloop(NULL);
    EXPECT_EQ(0, 0); // how do you test this ?
   }
*/

TEST_F(NeonISPMiscTest, DISABLED_test_utc_tester){
        const char * str = "2014-01-02T01:02:03Z";
        //time_t correct = 1388624523;
        time_t correct = 1388649723; // ?
        
        time_t result = 0;
        int ret = neon_convert_string_to_time(str, &result);
        
        EXPECT_EQ(ret, NEON_UTC_OK);
        EXPECT_EQ(correct, result);
}


TEST_F(NeonISPMiscTest, DISABLED_test_utils_get_expiry){
        time_t correct = 1388624523;
        time_t result = neon_get_expiry("mastermind.test");
        EXPECT_EQ(correct, result);
}
    
TEST_F(NeonISPMiscTest, DISABLED_test_utils_file_exist){
        int ret = neon_check_file_exist("mastermind.validated.test");
        EXPECT_EQ(ret, NEON_TRUE);
        
        ret = neon_check_file_exist("mastermind.invalid.test");
        EXPECT_EQ(ret, NEON_TRUE);
        
}
