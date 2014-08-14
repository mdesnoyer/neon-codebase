#ifndef _NEON_PUBLISHER_HASHTABLE_
#define _NEON_PUBLISHER_HASHTABLE_

#include <string>
#include <ext/hash_map>
#include "rapidjson/document.h"
#include "publisher.h"

class PublisherHashtable {
    
public:
    
    PublisherHashtable();
    ~PublisherHashtable();
    
    
    /*
     *
     */
    void Init(unsigned numOfBuckets);
    
    
    void Shutdown();
    
    
    unsigned GetSize();
    
    /*
     *
     */
    void AddPublisher(rapidjson::Document & publisher);

    
    
    Publisher * Find(const char * publisherIdKey);
    
    /*
     *
     */
    
    /*
    struct eq_pub
	{
        bool operator()(const Publisher * p1, const Publisher * p2) const
        {
            return p1->GetPublisherIdRef() == p2->GetPublisherIdRef();
        }
	};
    */

    /*
	struct hash_publisher {
        size_t operator()(const std::string key)  const {
           // unsigned long long ret = (in >> 32L) ^ (in & 0xFFFFFFFF);
            //return (size_t) ret;
            return 0;
        }
	};
    */
    
    
    struct hash_publisher {
        size_t operator()(const std::string key)  const;
	};


    typedef __gnu_cxx::hash_map<std::string, Publisher *, hash_publisher>  PublisherTable;


    PublisherTable * table;
};



#endif


