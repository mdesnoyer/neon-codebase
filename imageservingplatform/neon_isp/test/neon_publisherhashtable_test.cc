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
#include "../publisherHashtable.h"

using namespace std;                                                                 

class PublisherHashTableTest: public :: testing::Test{

public:
        PublisherHashTableTest(){}
protected:
        virtual void SetUp(){
        
        }
};

TEST_F(PublisherHashTableTest, test_pub_table){

        PublisherHashTable table;
        table.Init(10);
        
        table.AddPublisher("pub1", "acct1");
        table.AddPublisher("pub2", "acct2");
        table.AddPublisher("pub3", "acct3");
    
        std::string result;
        std::string correct = "acct2";
        PublisherHashTable::EFindError err = table.Find("pub2", result);
        EXPECT_EQ(result, correct);
}
