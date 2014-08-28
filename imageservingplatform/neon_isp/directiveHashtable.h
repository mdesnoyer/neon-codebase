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
    
    void Init(unsigned numOfBuckets);
    
    void Shutdown();
    
    unsigned GetSize();
    
    /*
     * Add a directive to the hash table
     */
    void AddDirective(rapidjson::Document & directive);

    
    /*
     * Directive hash table Enums 
     */
    enum EFindError {
        Found,
        NotFound,
        WrongArgument
    };
    
    const Directive * Find(std::string & accountId, std::string & videoId) const;
    
    static void ConstructKey(std::string & accountId, std::string & videoId, std::string *key);
    
protected:

    struct hash_directive {
        size_t operator()(const std::string & key)  const;
	};


    typedef __gnu_cxx::hash_map<std::string, Directive*, hash_directive>  DirectiveTable;
    
    DirectiveTable * table;
};



#endif


