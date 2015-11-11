#ifndef _NEON_DEFAULT_THUMBNAIL_HASH_TABLE_
#define _NEON_DEFAULT_THUMBNAIL_HASH_TABLE_


#include <string>
#include <ext/hash_map>
#include "stringHash.h" 
#include "rapidjson/document.h"
#include "defaultThumbnail.h"


class DefaultThumbnailHashtable {
    
public:
    
    DefaultThumbnailHashtable();
    ~DefaultThumbnailHashtable();
    
    void Init(unsigned numOfBuckets);
    
    void Shutdown();
    
    unsigned GetSize();
    
    /*
     * Add a default thumbnail directive
     */
    void Add(rapidjson::Document & directive);

    
    /*
     * Hash table Enums 
     */
    enum EFindError {
        Found,
        NotFound,
        WrongArgument
    };
    
    const DefaultThumbnail * Find(std::string & accountId) const;
    
    
protected:

    bool initialized;
    typedef __gnu_cxx::hash_map<std::string, DefaultThumbnail*>  DefaultThumbnailTable;
    
    DefaultThumbnailTable * table;
};



#endif


