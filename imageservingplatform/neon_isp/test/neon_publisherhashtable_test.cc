/*
 * Publisher Hashtable
 * */

#include <gtest/gtest.h>
#include <stdio.h>
#include <time.h>

#include "neon_error_codes.h"
#include "neon_updater.h"
#include "neon_utc.h"
#include "neon_utils.h"

#include "publisher.h"
#include "publisherHashtable.h"
#include "rapidjson/document.h"

using namespace std;                                                                 

class PublisherHashTableTest: public :: testing::Test{

public:
        PublisherHashTableTest(){}
protected:
        virtual void SetUp(){
        
        }
};

TEST_F(PublisherHashTableTest, test_pub_table){
        
        char * pub = "{\"pid\": \"pub1\", \"aid\" : \"acc1\" }\n";
                       //"{\"pid\": \"pub2\", \"aid\" : \"acc2\" }\n"
                       // "{\"pid\": \"pub3\", \"aid\" : \"acc3\" }\n";
        rapidjson::Document document;
        document.Parse<0>(pub);
        
        PublisherHashtable table;
        table.Init(3);
        table.AddPublisher(document);
        std::string correct = "acc1";
        Publisher * result = table.Find("pub1");
        EXPECT_STREQ(result->GetAccountId(), correct.c_str());
}
