#include <iostream>
#include "defaultThumbnailHashtable.h"
#include "neon_stats.h"

/*
 *   Directive Table
 */

DefaultThumbnailHashtable::DefaultThumbnailHashtable(){
    table = 0;
    initialized = false;
}

DefaultThumbnailHashtable::~DefaultThumbnailHashtable(){
    table = 0;
    initialized = false;
}

void
DefaultThumbnailHashtable::Init(unsigned numOfBuckets){

    if(initialized == true) {
        neon_stats[NEON_DEFAULT_THUMBNAIL_HASTABLE_INVALID_INIT]++;
        return;
    }
    
    table = new DefaultThumbnailTable(numOfBuckets);
    initialized = true;
}

void
DefaultThumbnailHashtable::Shutdown(){
 
    if(initialized == false) {
        neon_stats[NEON_DEFAULT_THUMBNAIL_HASTABLE_INVALID_SHUTDOWN]++;
        return;
    }

    if(table == 0)
        return;
    
    for(DefaultThumbnailTable::iterator it = table->begin(); it != table->end(); it ++)
    {

        DefaultThumbnail * d = (DefaultThumbnail *) ((*it).second);
        (*it).second = NULL;

        if(d == NULL) {
            neon_stats[NEON_DEFAULT_THUMBNAIL_SHUTDOWN_NULL_POINTER]++;
            continue;
        }

        d->Shutdown();
        delete d;
    }

    table->clear();

    delete table;
    table = 0;
    initialized = false;
}

unsigned
DefaultThumbnailHashtable::GetSize(){
    return table->size();
}

void
DefaultThumbnailHashtable::Add(rapidjson::Document & defaultThumb){
    
    DefaultThumbnail * d = new DefaultThumbnail();
    
    // object will dealloc any internal in case of failure 
    int ret = d->Init(defaultThumb);
    
    if(ret != 0) {
        // just delete it
        delete d;
        return;
    }
    
    (*table)[d->GetAccountId()] = d;
}

const DefaultThumbnail *
DefaultThumbnailHashtable::Find(std::string & accountId) const {

    DefaultThumbnail * def = NULL;
    def = (*table)[accountId];
    return def;
}
