#ifndef _NEON_PUBLISHER_HASHTABLE_
#define _NEON_PUBLISHER_HASHTABLE_

#include <string>
#include <ext/hash_map>
#include "stringHash.h"
#include "rapidjson/document.h"
#include "publisher.h"

class PublisherHashtable {
    
public:
    
    PublisherHashtable();
    ~PublisherHashtable();

    void Init(unsigned numOfBuckets);
    void Shutdown();
    unsigned GetSize();
    
    void AddPublisher(rapidjson::Document & publisher);
    Publisher * Find(const char * publisherIdKey);
    typedef __gnu_cxx::hash_map<std::string, Publisher *>  PublisherTable;
    bool initialized;

    PublisherTable * table;
};
#endif
