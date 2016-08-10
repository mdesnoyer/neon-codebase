/*
 * Directive class 
 *
 * This is class that reprents each of the directives from the
 * mastermind file. All directives are stored in the directiveHashtable
 * */

#ifndef _NEON_DIRECTIVE__
#define _NEON_DIRECTIVE__

#include <vector>
#include "fraction.h"
#include "neon_constants.h"
#include "rapidjson/document.h"


/*
 *  Directive type
 */
class Directive  {
    
    
public:
    
    Directive();
    Directive(const Directive &  p);
    ~Directive();
    
    int Init(const rapidjson::Document & document);
    
    void Shutdown();
    
    std::string GetKey() const;
    
    const char * GetAccountId() const;
    
    const std::string & GetAccountIdRef() const;
    
    const char * GetVideoId() const;
    
    const std::string & GetVideoIdRef() const;
    
    bool operator == (const Directive &other) const;
    
    const Fraction * GetFraction(unsigned char * bucketId, int bucketIdLen) const;
    
    Fraction * GetFraction(int index) {return fractions[index];};

    bool GetSendQueryString() const; 

protected:
    
    int InitSafe(const rapidjson::Document & document);
    void Dealloc();
    
    std::string  accountId;
    std::string  videoId;
    time_t sla;
    bool sendQueryString; 
    
    std::vector<Fraction*> fractions;
};


#endif

