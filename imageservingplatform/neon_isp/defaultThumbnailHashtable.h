#ifndef _NEON_DEFAULT_THUMBNAIL_HASH_TABLE_
#define _NEON_DEFAULT_THUMBNAIL_HASH_TABLE_


#include <string>
#include <ext/hash_map>
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

    struct hash_directive {
        size_t operator()(const std::string & key)  const;
	};

    typedef __gnu_cxx::hash_map<std::string, DefaultThumbnail*, hash_directive>  DefaultThumbnailTable;
    
    DefaultThumbnailTable * table;
};



#endif


