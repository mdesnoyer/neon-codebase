/*
 * Directive Hash table test
 */


#include <gtest/gtest.h>
#include <stdio.h>
#include <time.h>

#include "neon_error_codes.h"
#include "neon_updater.h"
#include "neon_utc.h"
#include "neon_utils.h"
#include "directiveHashtable.h"
#include "publisherHashtable.h"

using namespace std;                                                                 

class DirectiveHashtableTest: public :: testing::Test{

public:
        DirectiveHashtableTest(){}
protected:
        virtual void SetUp(){
        
        }
};


TEST_F(DirectiveHashtableTest, test_directive_table){
        
    char dir[] =
    "{                                                                      "
    "    \"twpe\":\"dir\",                                                  "
    "    \"aid\":\"acc1\",                                                  "
    "    \"vid\":\"vid1\",                                                  "
    "    \"sla\":\"2014-03-27T23:23:02Z\",                                  "
    "    \"fractions\":                                                     "
    "    [                                                                  "
    "         {                                                             "
    "             \"pct\": 0.9,                                             "
    "             \"default_url\":\"http://vid1\",                          "
    "             \"tid\":\"tid1\",                                         "
    "             \"imgs\":                                                 "
    "             [                                                         "
    "                  {                                                    "
    "                       \"h\":500,                                      "
    "                       \"w\":600,                                      "
    "                       \"url\":\"http://neon/thumb1_500_600.jpg\"      "
    "                  },                                                   "
    "                  {                                                    "
    "                       \"h\":700,                                      "
    "                       \"w\":800,                                      "
    "                       \"url\":\"http://neon/thumb2_700_800.jpg\"      "
    "                  }                                                    "
    "             ]                                                         "
    "         },                                                            "
    "         {                                                             "
    "             \"pct\": 0.1,                                             "
    "             \"default_url\":\"http://vid1\",                          "
    "             \"tid\":\"tid2\",                                         "
    "             \"imgs\":                                                 "
    "             [                                                         "
    "                  {                                                    "
    "                       \"h\":500,                                      "
    "                       \"w\":600,                                      "
    "                       \"url\":\"http://neon/thumb1_100_200.jpg\"      "
    "                  },                                                   "
    "                  {                                                    "
    "                       \"h\":700,                                      "
    "                       \"w\":800,                                      "
    "                       \"url\":\"http://neon/thumb2_300_400.jpg\"      "
    "                  }                                                    "
    "             ]                                                         "
    "         }                                                             "
    "     ]                                                                 "
    "}                                                                      "
    ;
        rapidjson::Document document;
        document.Parse<0>(dir);
        
        DirectiveHashtable table;
        table.Init(10);
        table.AddDirective(document);

        std::string acct = "acc1";
        std::string vid = "vid1";
        std::string key;
        key.append(acct);
        key.append(vid);
        
        const Directive * r1 = table.Find(acct, vid);
        
        EXPECT_EQ(r1->GetAccountId(), acct);
        EXPECT_EQ(r1->GetVideoId(), vid);

        //EXPECT_EQ(r1->GetKey(), key);
        
        table.Shutdown();
}
