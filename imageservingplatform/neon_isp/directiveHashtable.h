#ifndef _NEON_DIRECTIVE_HASH_TABLE_
#define _NEON_DIRECTIVE_HASH_TABLE_

#include <string>
#include <ext/hash_map>
#include "rapidjson/document.h"
#include "directive.h"

class DirectiveHashtable {
    
    
public:
    
    DirectiveHashtable();
    ~DirectiveHashtable();
    
    
    /*
     *
     */
    void Init(unsigned numOfBuckets);
    
    /*
     *
     */
    void Shutdown();
    
    unsigned GetSize();
    
    /*
     *
     */
    void AddDirective(rapidjson::Document & directive);

    
    /*
     *
     */
    enum EFindError {
        Found,
        NotFound,
        WrongArgument
    };
    
    const Directive * Find(std::string & accountId, std::string & videoId) const;
    
    
protected:

    /*
     *
     */
/*
    struct eq_directive
	{
        bool operator()(const unsigned long long int s1, const unsigned long long int s2) const
        {
            return true ? s1==s2: false;
        }
	};
*/
    
/*
	struct hash_directive {
        size_t operator()(const std::string key)  const {
            // unsigned long long ret = (in >> 32L) ^ (in & 0xFFFFFFFF);
            //return (size_t) ret;
            return (size_t) NeonHash::hash(key.c_str(), key.size);
        }
	};
*/
    
    struct hash_directive {
        size_t operator()(const std::string key)  const;
	};


    typedef __gnu_cxx::hash_map<std::string, Directive*, hash_directive>  DirectiveTable;
    
    DirectiveTable * table;
};



#endif


