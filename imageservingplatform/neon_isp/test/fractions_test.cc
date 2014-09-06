/*
 * Fractions class test
 */


#include <gtest/gtest.h>
#include <stdio.h>
#include <time.h>

#include "neon_error_codes.h"
#include "neon_updater.h"
#include "neon_utc.h"
#include "neon_utils.h"
#include "directive.h"
#include "fraction.h"

using namespace std;                                                                 

class FractionsTest: public :: testing::Test, public Directive{

public:
        FractionsTest(){}
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
            
            document.Parse<0>(dir);

        }
        rapidjson::Document document;
        Directive directive;
};


TEST_F(FractionsTest, test_parsing_single_fraction){

    directive.Init(document);
    Fraction *f = directive.GetFraction(0);
    EXPECT_EQ(0.9, f->GetPct());
    //EXPECT_EQ(,f->GetThreshold());
}

TEST_F(FractionsTest, test_scaled_image){

}

// Test Fraction size
//
// Test Rescaled fraction

