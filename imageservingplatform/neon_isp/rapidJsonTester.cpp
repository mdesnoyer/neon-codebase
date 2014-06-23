#include <time.h>
#include <stdio.h>
#include <assert.h>
#include <string>

#include "rapidjson/document.h"
#include <cstdio>



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






int
main(int argc, char ** argv)
{

    // publisher table
    {
        rapidjson::Document document;
        document.Parse<0>(pub);
        
        const rapidjson::Value & a = document["a"];
        
        assert(a.IsArray());
        std::string correct_pub = "publisher_id";
        std::string correct_acct = "account_id";
            
        rapidjson::SizeType index = 0;
        assert( a[index].GetString() == correct_pub);
        
        index ++;
        assert(a[1].GetString() == correct_acct);
            
    }
    
    
    
    // directives table
    {
        std::string correct;
        
        rapidjson::Document document;
        document.Parse<0>(dir);
        
        assert(document.IsObject());
        
        assert(document.HasMember("aid"));
        correct = "account1";
        assert(document["aid"].GetString() == correct);
        
        assert(document.HasMember("vid"));
        correct = "vid1";
        assert(document["vid"].GetString() == correct);

        assert(document.HasMember("expiry"));
        correct = "2014-03-27T23:23:02Z";
        assert(document["expiry"].GetString() == correct);
        
        
        const rapidjson::Value & fractions = document["fractions"];
        assert(fractions.IsArray());
        
        rapidjson::SizeType index = 0;
        
        const rapidjson::Value & frac1 =  fractions[index];
        
        assert(frac1.HasMember("pct"));
        assert(frac1["pct"].IsNumber());
        assert(frac1["pct"].IsDouble());
        double pct = 0.8;
        assert(frac1["pct"].GetDouble() == pct);
        
        assert(frac1.HasMember("imgs"));
        const rapidjson::Value & imgs = frac1["imgs"];
        assert(imgs.IsArray());
        
        
    }
    
    
}



