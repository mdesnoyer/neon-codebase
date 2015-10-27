#ifndef _NEON_PUBLISHER_HASH_TABLE_
#define _NEON_PUBLISHER_HASH_TABLE_

#include <string>
#include <ext/hash_map>
#include "stringHash.h" 
#include "publisher.h"

class PublisherHashTable {

public:
    PublisherHashTable();
    ~PublisherHashTable();

    enum EInitError {
        InitOk,
        InitFailWrongArgument
    };
    
    EInitError Init(unsigned numOfBuckets);
    
    enum EAddPublisherError {
        AddOk,
        AddWrongArgument
    };
    
    EAddPublisherError AddPublisher(const char * publisherId, const char * accountId);

    enum EFindError {
        Found,
        NotFound,
        WrongArgument
    };
    
    EFindError Find(const char * publisherIdKey, std::string & accountId);
    
    typedef __gnu_cxx::hash_map<std::string, Publisher>  PublisherTable;
    
    PublisherTable * table;
};
#endif
