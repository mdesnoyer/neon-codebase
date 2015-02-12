#ifndef _NEON_ACCOUNT_
#define _NEON_ACCOUNT_


#include <ext/hash_map>
#include "videoCounter.h"

class AccountHashtableLogger;


class Account {

    public:
            
    Account();
    ~Account();

    int Init(const char * aid);
    
    void Shutdown();
    
    void SetId(const char * aid);

    const std::string & GetId() const;

    void Increment(const char * videoId);

    VideoCounter * FindVideoCounter(const char * vid);

    int Traverse(AccountHashtableLogger * logger);

    static const unsigned numOfBuckets = 100;

    static Account * Create();

    static void Destroy(Account * a);


    protected:

    struct hash_video {
        size_t operator()(const std::string key)  const;
    };


    typedef __gnu_cxx::hash_map<std::string, VideoCounter *, hash_video>  VideoCounterTable;

    std::string accountId;

    bool initialized;

    VideoCounterTable * table;

};





#endif


