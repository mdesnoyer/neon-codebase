#include "account.h"
#include "neonHash.h"




Account::Account() 
{
}


Account::~Account() 
{
} 


int
Account::Init()
{
    if(initialized == true) {
        return 1;
    }

    table = new VideoCounterTable(numOfBuckets);
    return 0;
}


void
Account::Shutdown()
{   
    if(initialized == false) {
        return;
    }

    for(VideoCounterTable::iterator it = table->begin(); it != table->end(); it ++)
    {
        VideoCounter * v = (*it).second;
        (*it).second = 0;

        v->Shutdown();
        delete v;
    }

    delete table;
    table = 0;
    initialized = false;
}


void 
Account::SetId(const char * aid)
{
    accountId = aid;
}


void 
Account::Increment(const char * videoId)
{
    VideoCounter * v = (*table)[videoId];

    if(v == 0) {
        v = VideoCounter::Create();
        v->SetId(videoId);        
        (*table)[videoId] = v;
    }

    v->Increment();
}


size_t
Account::hash_video::operator()(const std::string key)  const
{
    uint32_t result = 0;

    result = NeonHash::Hash(key.c_str(), 1);

    return result;
};



Account * 
Account::Create()
{
    return new Account();
}


void
Account::Destroy(Account * a)
{
    delete a;
}



