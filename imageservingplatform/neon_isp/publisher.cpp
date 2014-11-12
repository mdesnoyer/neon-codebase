#include <iostream>
#include "publisher.h"




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
     //std::cout << "\nPublisher destructor";
}


int
Publisher::Init(const char* pub, const char* acc)
{
    if(pub == 0)
        return -1;
    
    if(strlen(pub) == 0)
        return -1;
    
    publisherId = pub;
    
    
    if(acc == 0)
        return -1;
    
    if(strlen(acc) == 0)
        return -1;
    
    accountId = acc;
}


void
Publisher::Shutdown()
{
    
    
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
