/*
 * Fractions class test
 */
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
#include "defaultThumbnail.h"
#include "test_utils.hpp"

using namespace std; 

class DefaultThumbnailTest: public :: testing::Test, public Directive{

    public:
        DefaultThumbnailTest(){}
    protected:
        virtual void SetUp(){
        }
};


TEST_F(DefaultThumbnailTest, test_default_thumbnail_default_url)
{
    string testString = TestUtils::readTestFile("defThumbBase.json"); 
    rapidjson::Document doc; 
    doc.Parse<0>(testString.c_str());  
    DefaultThumbnail dt; 
    dt.Init(doc); 
    const ScaledImage *si = dt.GetScaledImage(-1, -1); 
    ASSERT_EQ(NULL, si);  
}

TEST_F(DefaultThumbnailTest, test_get_exact_match_url) 
{
    string testString = TestUtils::readTestFile("defThumbBase.json"); 
    rapidjson::Document doc; 
    doc.Parse<0>(testString.c_str());  
    DefaultThumbnail dt; 
    dt.Init(doc); 
    const ScaledImage *si = dt.GetScaledImage(600, 800); 
    ASSERT_EQ(*si->scoped_url(), "http://neon/thumb_600_800_default_url_defaccount1.jpg"); 
}

TEST_F(DefaultThumbnailTest, test_get_approx_match_url) 
{ 
    string testString = TestUtils::readTestFile("defThumbBase.json"); 
    rapidjson::Document doc; 
    doc.Parse<0>(testString.c_str());  
    DefaultThumbnail dt; 
    dt.Init(doc); 
    const ScaledImage *si = dt.GetScaledImage(595, 803); 
    ASSERT_EQ(*si->scoped_url(), "http://neon/thumb_600_800_default_url_defaccount1.jpg"); 
}
