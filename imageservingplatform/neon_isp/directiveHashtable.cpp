#include <iostream>
#include "directiveHashtable.h"
#include "neonHash.h"


/*
 *   Directive Table
 */

DirectiveHashtable::DirectiveHashtable(){
    table = 0;
}


DirectiveHashtable::~DirectiveHashtable(){
    table = 0;
}


void
DirectiveHashtable::Init(unsigned numOfBuckets){

    if(initialized == true)
        return;
    
    table = new DirectiveTable(numOfBuckets);
    initialized = true;
}


void
DirectiveHashtable::Shutdown(){
 
    if(initialized == false)
        return;

    if(table == 0)
        return;
    
    for(DirectiveTable::iterator it = table->begin(); it != table->end(); it ++)
    {

        Directive * d = (Directive *) ((*it).second);
        (*it).second = NULL;

        if(d == NULL)
            continue;

        d->Shutdown();
        delete d;
    }

    table->clear();

    delete table;
	table = 0;
    initialized = false;
}


unsigned
DirectiveHashtable::GetSize(){
    return table->size();
}


void
DirectiveHashtable::AddDirective(rapidjson::Document & directive){
    
    Directive * d = new Directive();
    
    d->Init(directive);
    
    (*table)[d->GetKey()] = d;
}


const Directive *
DirectiveHashtable::Find(std::string & accountId, std::string & videoId) const {

    std::string key;
   
    DirectiveHashtable::ConstructKey(accountId, videoId, &key);
    Directive * directive = NULL;
    directive = (*table)[key];

    return directive;
}


size_t
DirectiveHashtable::hash_directive::operator()(const std::string & key)  const {
    uint32_t result = 0;
    
    result = NeonHash::Hash(key.c_str(), 1);
    
    return result;
}

void 
DirectiveHashtable::ConstructKey(std::string & accountId, std::string & videoId, std::string *key){
    
    (*key).append(accountId);
    (*key).append(videoId);

}
