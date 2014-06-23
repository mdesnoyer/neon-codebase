#include <time.h>
#include <stdio.h>
#include <assert.h>
#include <string>

#include "publisherHashtable.h"




int
main(int argc, char ** argv)
{
    
    {
        PublisherHashTable table;
        table.Init(10);
        
        table.AddPublisher("pub1", "acct1");
        table.AddPublisher("pub2", "acct2");
        table.AddPublisher("pub3", "acct3");
    
        std::string result;
        std::string correct = "acct2";
        PublisherHashTable::EFindError err = table.Find("pub2", result);
        assert(result == correct);
    }
}


