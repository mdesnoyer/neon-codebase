#ifndef _NEON_PUBLISHER__
#define _NEON_PUBLISHER__

#include <string>
#include "rapidjson/document.h"


/*
 *  Publisher entry type in hashtable
 */
class Publisher  {
    
    
public:
    
    Publisher();
    Publisher(const Publisher &  p);
    ~Publisher();
    
    void Init(const char* pub, const char* acc);
    
    void Shutdown();
    
    const char * GetPublisherId();
    
    const std::string & GetPublisherIdRef() const;
    
    const char * GetAccountId();
    
    const std::string & GetAccountIdRef() const;
    
    bool operator==(const Publisher &other) const;
    
protected:
    
    std::string  publisherId;
    std::string  accountId;
    
};


#endif



