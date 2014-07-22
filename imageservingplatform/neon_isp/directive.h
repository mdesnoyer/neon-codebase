/*
 * Directive class 
 *
 * This is class that reprents each of the directives from the
 * mastermind file. All directives are stored in the directiveHashtable
 * */

#ifndef _NEON_DIRECTIVE__
#define _NEON_DIRECTIVE__

#include <vector>
#include "rapidjson/document.h"
#include "fraction.h"


/*
 *  Directive type
 */
class Directive  {
    
    
public:
    
    Directive();
    Directive(const Directive &  p);
    ~Directive();
    
    void Init(rapidjson::Document & document);
    
    void Shutdown();
    
    std::string GetKey() const;
    
    const char * GetAccountId() const;
    
    const std::string & GetAccountIdRef() const;
    
    const char * GetVideoId() const;
    
    const std::string & GetVideoIdRef() const;
    
    bool operator == (const Directive &other) const;
    
    const Fraction * GetFraction(unsigned char * hash_string, int hash_string_len) const;
    
    Fraction * GetFraction(int index) {return fractions[index];};

protected:
    
    std::string  accountId;
    std::string  videoId;
    time_t sla;
    
    std::vector<Fraction*> fractions;
};


#endif

