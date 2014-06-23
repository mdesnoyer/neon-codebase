#include <iostream>
#include "neonHash.h"
#include "directiveHashtable.h"


/*
 *   Directive Table
 */

DirectiveHashtable::DirectiveHashtable()
{
    table = 0;
}


DirectiveHashtable::~DirectiveHashtable()
{
        table = 0;
}


void
DirectiveHashtable::Init(unsigned numOfBuckets)
{
    table = new DirectiveTable(numOfBuckets);
}


void
DirectiveHashtable::Shutdown()
{
    if(table == 0)
        return;
    
    for(DirectiveTable::iterator it = table->begin(); it != table->end(); it ++)
    {
        ((*it).second)->Shutdown();
        delete (*it).second;
    }

    delete table;
	table = 0;
}


unsigned
DirectiveHashtable::GetSize()
{
    return table->size();
}


void
DirectiveHashtable::AddDirective(rapidjson::Document & directive)
{
    Directive * d = new Directive();
    
    d->Init(directive);
    
    (*table)[d->GetKey()] = d;
}


const Directive *
DirectiveHashtable::Find(std::string & accountId, std::string & videoId) const
{
    std::string key;
    key.append(accountId);
    key.append(videoId);
    
    Directive * directive = 0;
    directive = (*table)[key];

    return directive;
}


size_t
DirectiveHashtable::hash_directive::operator()(const std::string key)  const
{
    uint32_t result = 0;
    
    result = NeonHash::Hash(key.c_str(), 1);
    
    return result;
};





