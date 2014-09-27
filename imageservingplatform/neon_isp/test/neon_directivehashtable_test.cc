/*
 * Directive Hash table test
 */


#include <gtest/gtest.h>
#include <stdio.h>
#include <time.h>

#include "directiveHashtable.h"
#include "neon_error_codes.h"
#include "neon_updater.h"
#include "neon_utc.h"
#include "neon_utils.h"
#include "publisherHashtable.h"

using namespace std;                                                                 

class DirectiveHashtableTest: public :: testing::Test{

public:
        DirectiveHashtableTest(){}
protected:
        virtual void SetUp(){
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

            table.Init(10);
            table.AddDirective(document);

        }
        DirectiveHashtable table;
};


// Test basic operations on hash table
TEST_F(DirectiveHashtableTest, test_directive_table){

        std::string acct = "acc1";
        std::string vid = "vid1";
        
        const Directive * r1 = table.Find(acct, vid);
        
        EXPECT_EQ(r1->GetAccountId(), acct);
        EXPECT_EQ(r1->GetVideoId(), vid);
        
}

// Test invalid account id 
TEST_F(DirectiveHashtableTest, test_invalid_acc_key){

        std::string acct = "_acc1";
        std::string vid = "vid1";
        
        const Directive * r1 = table.Find(acct, vid);
        
        EXPECT_TRUE(r1 == NULL);
        
}

// Test BucketId Logic
TEST_F(DirectiveHashtableTest, test_bucketid_logic){

        std::string acct = "acc1";
        std::string vid = "vid1";
        
        const Directive * r1 = table.Find(acct, vid);
       
        unsigned char * bId = (unsigned char *) "10";
        int bIdLen = 2;

        // Case: get majority fraction
        const Fraction * f = r1->GetFraction(bId, bIdLen);
        EXPECT_DOUBLE_EQ(f->GetPct(), 0.9);
      
        // Bucket Ids are in hex
        
        bId = (unsigned char *) "50";
        f = r1->GetFraction(bId, bIdLen);
        EXPECT_DOUBLE_EQ(f->GetPct(), 0.9);
        
        // Case: get minority fraction 
        bId = (unsigned char *) "384";
        f = r1->GetFraction(bId, bIdLen);
        EXPECT_DOUBLE_EQ(f->GetPct(), 0.1);
        
        bId = (unsigned char *) "3E7";
        f = r1->GetFraction(bId, bIdLen);
        EXPECT_DOUBLE_EQ(f->GetPct(), 0.1);

        // Case: bucketId len = 0 
        f = r1->GetFraction(bId, 0);
        EXPECT_DOUBLE_EQ(f->GetPct(), 0.9);
        
        // Case: Garbage bucketId
        bId = (unsigned char *) "xags"; 
        bIdLen = 4;
        f = r1->GetFraction(bId, bIdLen);
        EXPECT_DOUBLE_EQ(f->GetPct(), 0.9);
}

