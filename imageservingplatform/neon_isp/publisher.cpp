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


void
Publisher::Init(const char* pub, const char* acc)
{
    publisherId = pub;
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
