#ifndef _NEON_ACCOUNT_HASHTABLE_
#define _NEON_ACCOUNT_HASHTABLE_


#include <string>
#include <ext/hash_map>
#include "rapidjson/document.h"
#include "account.h"
#include "videoCounter.h"


class accountHashtableLogger;


class AccountHashtable {

public:

    AccountHashtable();
    ~AccountHashtable();

    int Init(unsigned numOfBuckets);

    void Shutdown();

    unsigned GetSize();

    void Increment(const char * accountId,
                   const char * videoId);

    VideoCounter * FindVideoCounter(const char * accountId, const char * videoId);

    int Traverse(AccountHashtableLogger * logger);   


protected:  

    struct hash_account {
        size_t operator()(const std::string key)  const;
    };

    typedef __gnu_cxx::hash_map<std::string, Account *, hash_account>  AccountTable;

    bool initialized;

    AccountTable * table;
};



#endif


