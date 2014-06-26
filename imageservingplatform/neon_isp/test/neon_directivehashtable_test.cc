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
#include "../directiveHashtable.h"
#include "../publisherHashtable.h"

using namespace std;                                                                 

class DirectiveHashTableTest: public :: testing::Test{

public:
        DirectiveHashTableTest(){}
protected:
        virtual void SetUp(){
        
        }
};


TEST_F(DirectiveHashTableTest, test_directive_table){
        
        DirectiveHashTable table;
        table.Init(10);
        
        Directive * d1 = new Directive();
        d1->Init("acct1", "vid1");
        table.AddDirective(d1);
        
        
        Directive * d2 = new Directive();
        d2->Init("acct2", "vid2");
        table.AddDirective(d2);
    
        
        Directive * r1 = 0;
        std::string acct = "acct1";
        std::string vid = "vid1";
        std::string key;
        key.append(acct);
        key.append(vid);
        
        table.Find(acct, vid, r1);
        EXPECT_EQ(r1->GetAccountId(), acct);
        EXPECT_EQ(r1->GetVideoId(), vid);
        EXPECT_EQ(r1->GetKey(), key);
        
        table.Shutdown();
}
