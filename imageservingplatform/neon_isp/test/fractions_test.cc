/*
 * Fractions class test
 */


#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>
#include <stdio.h>
#include <libgen.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <fstream> 
#include <time.h>

#include "neon_error_codes.h"
#include "neon_updater.h"
#include "neon_utc.h"
#include "neon_utils.h"
#include "directive.h"
#include "fraction.h"
#include "test_utils.hpp"

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

// This test likely belongs in a url_utils test
TEST_F(FractionsTest, test_generate_default_url_base) 
{
    string testString = TestUtils::readTestFile("noUrlsGoodDirective.json"); 
    rapidjson::Document doc; 
    doc.Parse<0>(testString.c_str());  
    rapidjson::Value& frac = doc["fractions"][0u];

    Fraction f; 
    f.Init(0,frac);
    string defaultUrl = url_utils::GenerateUrl(f.base_url(), (std::string)f.GetThumbnailID(),700,800); 

    ASSERT_EQ("http://kevin_test/neontnthumb1_w800_h700.jpg", defaultUrl); 
}

TEST_F(FractionsTest, test_frac_init_no_height) 
{
    string testString = TestUtils::readTestFile("lackingHeightDirective.json"); 
    rapidjson::Document doc; 
    doc.Parse<0>(testString.c_str());  
    rapidjson::Value& frac = doc["fractions"][0u];
    Fraction f; 
    int rv = f.Init(0, frac); 
    ASSERT_EQ(rv, -1); 
}

TEST_F(FractionsTest, test_frac_init_no_width) 
{
    string testString = TestUtils::readTestFile("lackingWidthDirective.json"); 
    rapidjson::Document doc; 
    doc.Parse<0>(testString.c_str());  
    rapidjson::Value& frac = doc["fractions"][0u];
    Fraction f; 
    int rv = f.Init(0, frac); 
    ASSERT_EQ(rv, -1); 
}

TEST_F(FractionsTest, test_frac_init_no_default_size) 
{
    string testString = TestUtils::readTestFile("lackingDefaultSizeDirective.json"); 
    rapidjson::Document doc; 
    doc.Parse<0>(testString.c_str());  
    rapidjson::Value& frac = doc["fractions"][0u];
    Fraction f; 
    int rv = f.Init(0, frac); 
    ASSERT_EQ(rv, -1); 
}

TEST_F(FractionsTest, test_generate_default_url_full) 
{ 
    string testString = TestUtils::readTestFile("noUrlsGoodDirective.json"); 
    rapidjson::Document doc; 
    doc.Parse<0>(testString.c_str());  
    rapidjson::Value& frac = doc["fractions"][0u];

    Fraction f; 
    f.Init(0,frac);
    ASSERT_EQ("http://kevin_test/neontnthumb1_w800_h700.jpg", *f.default_url()); 
}
