#include "accountHashtable.h"
#include "accountHashtableLogger.h"
#include "neonHash.h"


AccountHashtable::AccountHashtable()
{
    table = 0;
    initialized = false;
}


AccountHashtable::~AccountHashtable()
{
    table = 0;
    initialized = false;
}


int
AccountHashtable::Init(unsigned numOfBuckets)
{
    if(initialized == true) {
        return 0;
    }

    table = new AccountTable(numOfBuckets);

    initialized = true;

    return 0;
}


void 
AccountHashtable::Shutdown()
{
    if(initialized == false) {
        return;
    }

    for(AccountTable::iterator it = table->begin(); it != table->end(); it ++)
    {
        Account * a = (*it).second;
        (*it).second = 0;

        if(a == NULL) {
            continue;
        }

        a->Shutdown();
        delete a;
    }

    delete table;
    table = 0;
    initialized = false;
}


unsigned 
AccountHashtable::GetSize()
{
    return table->size();
}


void 
AccountHashtable::Increment(const char * accountId,
                            const char * videoId)
{
    Account * a = (*table)[accountId];

    if(a == 0) {
        a = Account::Create();
        a->SetId(accountId);
        (*table)[accountId] = a;
    }

    a->Increment(videoId);
}


VideoCounter * 
AccountHashtable::FindVideoCounter(const char * aid, const char * vid)
{
     Account * a = (*table)[aid];

    if(a == 0)
        return 0;

    


    return 0;
}


int 
AccountHashtable::Traverse(AccountHashtableLogger * logger)
{
    for(AccountTable::iterator it = table->begin(); it != table->end(); it ++)
    {
        (*it).second->Traverse(logger);
    }
    return 0;
}


size_t
AccountHashtable::hash_account::operator()(const std::string key)  const
{
    uint32_t result = 0;

    result = NeonHash::Hash(key.c_str(), 1);

    return result;
};


