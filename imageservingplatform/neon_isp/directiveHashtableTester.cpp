#include <time.h>
#include <stdio.h>
#include <assert.h>
#include <string>
#include "directiveHashtable.h"


void
test_directive_hashtable(){


    {
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
        assert(r1->GetAccountId() == acct);
        assert(r1->GetVideoId() == vid);
        assert(r1->GetKey() == key);
        
        table.Shutdown();
        
    }
}

int
main(int argc, char ** argv)
{
	test_directive_hashtable();    
}


