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
                "{  "
                "\"type\":\"dir\",  "
                "\"aid\":\"acc1\",  "
                "\"vid\":\"vid1\",  "
                "\"sla\":\"2014-03-27T23:23:02Z\",  "
                "\"fractions\": "
                "[  "
                " { "
                " \"pct\": 0.9, "
                " \"default_url\":\"http://vid1\",  "
                " \"tid\":\"tid1\", "
                " \"imgs\": "
                " [ "
                "  {"
                "   \"h\":500,  "
                "   \"w\":600,  "
                "   \"url\":\"http://neon/thumb1_500_600.jpg\"  "
                "  },   "
                "  {"
                "   \"h\":700,  "
                "   \"w\":800,  "
                "   \"url\":\"http://neon/thumb2_700_800.jpg\"  "
                "  }"
                " ] "
                " },"
                " { "
                " \"pct\": 0.1, "
                " \"default_url\":\"http://vid1\",  "
                " \"tid\":\"tid2\", "
                " \"imgs\": "
                " [ "
                "  {"
                "   \"h\":500,  "
                "   \"w\":600,  "
                "   \"url\":\"http://neon/thumb1_100_200.jpg\"  "
                "  },   "
                "  {"
                "   \"h\":700,  "
                "   \"w\":800,  "
                "   \"url\":\"http://neon/thumb2_300_400.jpg\"  "
                "  }"
                " ] "
                " } "
                " ] "
                "}  "
                ;

            document.Parse<0>(dir);

        }
        rapidjson::Document document;
        Directive directive;
};


TEST_F(FractionsTest, test_parsing_single_fraction){

    directive.Init(document);
    Fraction *f = directive.GetFraction(0);
    EXPECT_DOUBLE_EQ(0.9, f->GetPct());
}

// Test Rescaled fractions
// Modify document to have un balanced fractions & ensure that they
// get renormalized accurately

TEST_F(FractionsTest, test_fractions_greater_than_one){
   
    // Modify document such that sum(pcts) > 1.0
    rapidjson::Value& fractions = document["fractions"];
    rapidjson::SizeType i = 1;
    fractions[i]["pct"] = 0.3;

    directive.Init(document);
    Fraction *f = directive.GetFraction(0);
    
    // Check rebalanced values
    EXPECT_DOUBLE_EQ(0.75, f->GetPct());
    f = directive.GetFraction(1);
    EXPECT_DOUBLE_EQ(0.25, f->GetPct());

}

TEST_F(FractionsTest, test_fractions_less_than_one){
   
    // Modify document such that sum(pcts) < 1.0
    rapidjson::Value& fractions = document["fractions"];
    rapidjson::SizeType i = 0;
    fractions[i]["pct"] = 0.8;
    i = 1;
    fractions[i]["pct"] = 0.1;

    directive.Init(document);
    Fraction *f = directive.GetFraction(0);
    
    // Check rebalanced values
    EXPECT_DOUBLE_EQ(0.8888888888888889, f->GetPct());
    f = directive.GetFraction(1);
    EXPECT_DOUBLE_EQ(0.1111111111111111, f->GetPct());

}

TEST_F(FractionsTest, test_approx_equal){
    int vals[5] = {498, 499, 500, 501, 502};

    for(int i=0; i<5; i++)
        ASSERT_TRUE(Fraction::ApproxEqual(500, vals[i], 2)); 

    ASSERT_TRUE(Fraction::ApproxEqual(500, 500, 0)); 
    ASSERT_FALSE(Fraction::ApproxEqual(500, 503, 2)); 
}
