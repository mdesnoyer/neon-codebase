#ifndef _NEON_PUBLISHER_HASH_TABLE_
#define _NEON_PUBLISHER_HASH_TABLE_

#include <string>
#include <ext/hash_map>
#include "neonPublisherHashtable.h"

/*
 *  Publisher entry type in hashtable
 */
class Publisher  {


public:
    
    Publisher();
    Publisher(const Publisher &  p);
    ~Publisher();
    
    void Init(const char* pub, const char* acc);
    
    const char * GetPublisherId();
    
    const std::string & GetPublisherIdRef() const;
    
    const char * GetAccountId();
    
    const std::string & GetAccountIdRef() const;
    
    bool operator==(const Publisher &other) const;
    
protected:
    
    std::string  publisherId;
    std::string  accountId;

};



namespace __gnu_cxx {
    template <> struct hash<std::string> {
        size_t operator()(std::string const & s) const {
            hash<const char *> h;
            return h(s.c_str());
        }
    };
}


class PublisherHashTable {
    
    
public:
    
    PublisherHashTable();
    ~PublisherHashTable();
    
    
    /*
     *
     */
    enum EInitError {
        InitOk,
        InitFailWrongArgument
    };
    
    EInitError Init(unsigned numOfBuckets);
    
    
    /*
     *
     */
    enum EAddPublisherError {
        AddOk,
        AddWrongArgument
    };
    
    EAddPublisherError AddPublisher(const char * publisherId, const char * accountId);

    
    /*
     *
     */
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


