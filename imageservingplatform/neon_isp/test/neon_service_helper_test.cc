/*
 * Tests for service helper  
 *
 * Currently all functions that can be tested independenly
 * without nginx core, are in the helper.c file
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
#include "neon_service_helper.c"
  
using namespace std;                                                                 
  
class NeonServiceTest: public ::testing::Test {                                   
public:                                                                              
  NeonServiceTest() {
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


TEST_F(NeonServiceTest, test_parse_number){

    ngx_str_t v = ngx_string("123");
    long ret = neon_service_parse_number(&v);
    EXPECT_EQ(ret, 123);
    
    v = ngx_string("xzyb");
    ret = neon_service_parse_number(&v);
    EXPECT_EQ(ret, -1);
    
    v = ngx_string("z1");
    ret = neon_service_parse_number(&v);
    EXPECT_EQ(ret, -1);
}

// Test the search headers function
//TEST_F(NeonServiceTest, test_search_headers_in){
//
//    ngx_http_request_t request;
//    ngx_table_elt_t header_x;
//}
