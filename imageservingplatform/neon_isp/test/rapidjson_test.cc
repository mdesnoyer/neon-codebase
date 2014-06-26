/*
 * Rapidjson test
 *
 * */

#include <gtest/gtest.h>
#include <stdio.h>
#include <string>
#include <time.h>

#include "rapidjson/document.h"

using namespace std;                                                                 

class RapidjsonTest: public :: testing::Test{

public:
        RapidjsonTest(){}
proteced:
        virtual void SetUp(){
        

char * pub = "{\"pid\": \"publisher_id\", \"aid\" : \"account_id\" }";

char dir[] =

"{                                                                      "
"    \"aid\":\"account1\",                                              "
"    \"vid\":\"vid1\",                                                  "
"    \"exp\":\"2014-03-27T23:23:02Z\",                                  "
"    \"fractions\":                                                     "
"    [                                                                  "
"         {                                                             "
"             \"pct\": 0.8,                                             "
"             \"imgs\":                                                 "
"             [                                                         "
"                  {                                                    "
"                       \"x\":\"500\",                                  "
"                       \"y\":\"600\",                                  "
"                       \"url\":\"http://neon/thumb1_500_600.jpg\"      "
"                  },                                                   "
"                  {                                                    "
"                       \"x\":\"700\",                                  "
"                       \"y\":\"800\",                                  "
"                       \"url\":\"http://neon/thumb2_700_800.jpg\"      "
"                  }                                                    "
"             ]                                                         "
"         },                                                            "
"         {                                                             "
"             \"pct\": 0.9,                                             "
"             \"imgs\":                                                 "
"             [                                                         "
"                  {                                                    "
"                       \"x\":\"100\",                                  "
"                       \"y\":\"200\",                                  "
"                       \"url\":\"http://neon/thumb1_100_200.jpg\"      "
"                  },                                                   "
"                  {                                                    "
"                       \"x\":\"300\",                                  "
"                       \"y\":\"400\",                                  "
"                       \"url\":\"http://neon/thumb2_300_400.jpg\"      "
"                  }                                                    "
"             ]                                                         "
"         }                                                             "
"     ]                                                                 "
"}                                                                      "
;

}
};

TEST_F(RapidjsonTest, test_json){
    // publisher table
    {
        rapidjson::Document document;
        document.Parse<0>(pub);
        
        const rapidjson::Value & a = document["a"];
        
        EXPECT_TRUE(a.IsArray());
        std::string correct_pub = "publisher_id";
        std::string correct_acct = "account_id";
            
        rapidjson::SizeType index = 0;
        EXPECT_STREQ(a[index].GetString(), correct_pub);
        
        index ++;
        EXPECT_STREQ(a[1].GetString(), correct_acct);
            
    }
    
    
    // directives table
    {
        std::string correct;
        
        rapidjson::Document document;
        document.Parse<0>(dir);
        
        EXPECT_TRUE(document.IsObject());
        
        EXPECT_TRUE(document.HasMember("aid"));
        correct = "account1";
        EXPECT_STREQ(document["aid"].GetString(), correct);
        
        EXPECT_TRUE(document.HasMember("vid"));
        correct = "vid1";
        EXPECT_STREQ(document["vid"].GetString(), correct);

        EXPECT_TRUE(document.HasMember("expiry"));
        correct = "2014-03-27T23:23:02Z";
        EXPECT_STREQ(document["expiry"].GetString(), correct);
        
        const rapidjson::Value & fractions = document["fractions"];
        EXPECT_TRUE(fractions.IsArray());
        
        rapidjson::SizeType index = 0;
        
        const rapidjson::Value & frac1 =  fractions[index];
        
        EXPECT_TRUE(frac1.HasMember("pct"));
        EXPECT_TRUE(frac1["pct"].IsNumber());
        EXPECT_TRUE(frac1["pct"].IsDouble());
        double pct = 0.8;
        EXPECT_EQ(frac1["pct"].GetDouble(), pct);
        
        EXPECT_TRUE(frac1.HasMember("imgs"));
        const rapidjson::Value & imgs = frac1["imgs"];
        EXPECT_TRUE(imgs.IsArray());
        
        
    }
    
}



