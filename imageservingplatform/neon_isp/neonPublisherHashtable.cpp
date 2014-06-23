#include "neonPublisherHashtable.h"



/*
 *   Publisher
 */

Publisher::Publisher()
{
    // left empty
}


Publisher::Publisher(const Publisher &  p)
{
    publisherId = p.GetPublisherIdRef();
    accountId = p.GetAccountIdRef();
}


Publisher::~Publisher()
{
    // left empty
}


void
Publisher::Init(const char* pub, const char* acc)
{
    publisherId = pub;
    accountId = acc;
}


const char *
Publisher::GetPublisherId()
{
    return publisherId.c_str();
}


const std::string &
Publisher::GetPublisherIdRef() const
{
    return publisherId;
}

const char *
Publisher::GetAccountId()
{
    return accountId.c_str();
}


const std::string &
Publisher::GetAccountIdRef() const
{
    return accountId;
}


bool
Publisher::operator==(const Publisher &other) const {
    
    return publisherId == other.GetPublisherIdRef();
};


/*
 *   Publisher Table
 */

PublisherHashTable::PublisherHashTable()
{}


PublisherHashTable::~PublisherHashTable()
{
    
    
}


PublisherHashTable::EInitError
PublisherHashTable::Init(unsigned numOfBuckets)
{
    
    table = new PublisherTable(numOfBuckets);
    
    return InitOk;
}


PublisherHashTable::EAddPublisherError
PublisherHashTable::AddPublisher(const char * publisherId, const char * accountId)
{
    Publisher p;
    p.Init(publisherId, accountId);
    
    (*table)[p.GetPublisherIdRef()] = p;
    return AddOk;
}


PublisherHashTable::EFindError
PublisherHashTable::Find(const char * publisherIdKey, std::string & accountId)
{
    std::string key = publisherIdKey;
    accountId = ((*table)[key]).GetAccountIdRef();
    
    if(accountId != "")
        return Found;
    
    return NotFound;
}





