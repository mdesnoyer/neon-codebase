#ifndef _NEON_MASTERMIND_CPP
#define _NEON_MASTERMIND_CPP

#include <time.h>
#include <stdio.h>
#include "rapidjson/document.h"
#include "publisherHashtable.h"
#include "directiveHashtable.h"


class Mastermind  {
    
    
public:
    
    Mastermind();
    ~Mastermind();

    // basic init with empty tables
    void Init();

    // full init with a json document
    void Init(const char * mastermindFile, time_t previousMastermindExpiry);
    
    void Shutdown();
    
    time_t GetExpiry();
    
    static const unsigned UTCStrSize = 20;
    
    static time_t GetFileExpiry(const char * const filepath);

    static  time_t GetFileExpiry(FILE * file);
    
    static time_t GetUTC(const char * s);
    
    static time_t ConvertUTC(const char * s);
    
    static double randZeroToOne();
    
    static const int MaxLineBufferSize = 40000;
    static char lineBuffer[];

    // searches the publisher hashtable
    const char * GetAccountId(const char * publisherId, int & size);
    
    const char * GetImageUrl(const char * accountId, 
                                const char * videoId, 
                                unsigned char * bucketId,
                                int bucketIdLen,
                                int height, 
                                int width, 
                                int & size);
    
    const char * GetThumbnailID(const char * c_accountId, 
                                const char * c_videoId, 
                                unsigned char * bucketId,
                                int bucketIdLen,
                                int &size);
   

protected:
    
    // this function reclaims all memory safely
    void Dealloc();
    void InitSafe(const char * mastermindFile, time_t previousMastermindExpiry);

    bool initialized;
    FILE * parseFile;

    static const std::string typeKey;
    static const std::string typeDirective;
    static const std::string typePublisher;
    
    static const std::string expiryKey;
    
    time_t expiry;

    PublisherHashtable * publisherTable;
    DirectiveHashtable * directiveTable;
};



/*
 *   hash table function
 */


using namespace std;
using namespace __gnu_cxx;


namespace __gnu_cxx {
    template<>
    struct hash<std::string>
    {
        hash<char*> h;
        size_t operator()(const std::string &s) const
        {
            return h(s.c_str());
        };
    };
}


#endif


