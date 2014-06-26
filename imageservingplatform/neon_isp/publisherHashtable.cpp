#include "neonHash.h"
#include "publisherHashtable.h"



/*
 *   Publisher Table
 */

PublisherHashtable::PublisherHashtable()
{
    table = 0;
}


PublisherHashtable::~PublisherHashtable()
{
    table = 0;
}


void
PublisherHashtable::Init(unsigned numOfBuckets)
{
    table = new PublisherTable(numOfBuckets);
}


void
PublisherHashtable::Shutdown()
{
    if(table == 0)
        return;
    
    for(PublisherTable::iterator it = table->begin(); it != table->end(); it ++)
    {
        ((*it).second)->Shutdown();
        delete (*it).second;
    }
    
    delete table;
	table = 0;
}


unsigned
PublisherHashtable::GetSize()
{
    return table->size();
}


void
PublisherHashtable::AddPublisher(rapidjson::Document & document)
{
    
    Publisher * p = new Publisher();
    
    // get publisher id
    document.HasMember("pid");
    // document["pid"].IsString());
    const char * publisherId = document["pid"].GetString();
    
    // get account id
    document.HasMember("aid");
    // document["pid"].IsString());
    const char * accountId = document["aid"].GetString();
    
    p->Init(publisherId, accountId);
    std::string key = publisherId;
    
    (*table)[key] = p;
	return;
}


Publisher *
PublisherHashtable::Find(const char * publisherIdKey)
{
    std::string key = publisherIdKey;
   
    Publisher * publisher = (*table)[key];
    
    if(publisher == 0)
        return 0;
    
    return publisher;
}


size_t
PublisherHashtable::hash_publisher::operator()(const std::string key)  const
{
    uint32_t result = 0;
    
    result = NeonHash::Hash(key.c_str(), 1);
    
    return result;
};



