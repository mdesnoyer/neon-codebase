/*
 * Neon Mastermind Interface 
 *
 * Methods used by the Neon module
 *
 * Manage the mastermind file (previous copy and current)
 *
 * Lookup methods in to mastermind data structure: AccountId, ImageURL
 * */

#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <exception>
#include "neon_stats.h"
#include "neonLog.h"
#include "neon_mastermind.h"
#include "neonException.h"
#include "mastermind.h"


static Mastermind * mastermind_current = 0;
static Mastermind * mastermind_old = 0;


Mastermind *
neon_get_mastermind()
{
    return  mastermind_current;
}


static NEON_BOOLEAN
deallocate_mastermind(Mastermind * m)
{
    if(m == 0)
        return NEON_TRUE;
    
    m->Shutdown();
    
    delete m;
    return NEON_TRUE;
}


NEON_BOOLEAN
neon_mastermind_init()
{
    Mastermind::InitStatic();

    mastermind_current = new Mastermind();
    mastermind_old = new Mastermind();
    
    return NEON_TRUE;
}


const char * neon_load_error = 0;

NEON_LOAD_ERROR
neon_mastermind_load(const char * filepath)
{
    Mastermind * to_delete = 0;
    Mastermind * candidate = 0;
    
    // recover memory of older mastermind before
    // processing a new one
    to_delete = mastermind_old;
    mastermind_old = 0;
    deallocate_mastermind(to_delete);
    
    // parse
    try {
        candidate = new Mastermind();
        
        candidate->Init(filepath, mastermind_current->GetExpiry());
    }
    catch (NeonException * error)
    {
        // log
        NeonLog::Error("neon_mastermind_load: %s", error->GetMessage());
        
        delete error;
        
        // erase candidate
        if(candidate)
            delete candidate;
        
        return NEON_LOAD_FAIL;
    }
    catch (std::bad_alloc e) {
        
        // log here
        NeonLog::Error("neon_mastermind_load: bad_alloc, out of memory");
        
        // erase candidate
        if(candidate)
            delete candidate;
        
        return NEON_LOAD_FAIL;
    }
    catch (...) {
        
        // log here
        NeonLog::Error("neon_mastermind_load: unspecified exception");
        
        // erase candidate
        if(candidate)
            delete candidate;
    
        return NEON_LOAD_FAIL;
    }

    // replace
    mastermind_old = mastermind_current;
    mastermind_current = candidate;
    
    return NEON_LOAD_OK;
}



NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_ERROR
neon_mastermind_account_id_lookup(const char * publisher_id,
                                  const char ** account_id,
                                  int * account_id_size)
{

    Mastermind * mastermind = neon_get_mastermind();
    
    if(mastermind_current == 0)
        return NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_FAIL;
    
    // find account id
    (*account_id) = mastermind->GetAccountId(publisher_id, *account_id_size);
    
    if(*account_id == 0)
        return NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_NOT_FOUND;

    return NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_OK;
}


/*
 * Lookup logic
 */
NEON_MASTERMIND_IMAGE_URL_LOOKUP_ERROR
neon_mastermind_image_url_lookup(const char * accountId,
        const char * videoId,
        ngx_str_t * ipAddress,
        int height,
        int width,
        const char ** url,
        int * size)
{
    Mastermind * mastermind = neon_get_mastermind();
    
    if(mastermind_current == 0)
        return NEON_MASTERMIND_IMAGE_URL_LOOKUP_FAIL;

    
    (*url) = mastermind->GetImageUrl(accountId, videoId, 
            ipAddress->data, ipAddress->len, 
            height, width, *size);
    
    if(*url == 0)
        return NEON_MASTERMIND_IMAGE_URL_LOOKUP_NOT_FOUND;
    

    return NEON_MASTERMIND_IMAGE_URL_LOOKUP_OK;
}

/*
 * TID lookup Interface method 
 *
 * */

NEON_MASTERMIND_TID_LOOKUP_ERROR
neon_mastermind_tid_lookup(const char * accountId,
        const char * videoId,
        ngx_str_t * ipAddress,
        const char ** tid,
        int * size)
{

    Mastermind * mastermind = neon_get_mastermind();
    
    if(mastermind_current == 0)
        return NEON_MASTERMIND_TID_LOOKUP_FAIL;

    
    (*tid) = mastermind->GetThumbnailID(accountId, videoId, ipAddress->data, ipAddress->len, *size); 

    
    if(*tid == 0)
        return NEON_MASTERMIND_TID_LOOKUP_NOT_FOUND;
    

    return NEON_MASTERMIND_TID_LOOKUP_OK;
}

NEON_BOOLEAN
neon_mastermind_expired()
{
    if(mastermind_current == NULL)
        return NEON_TRUE;
    
    time_t now = time(0);
    
    if(now >= mastermind_current->GetExpiry())
        return NEON_TRUE;
    
    return NEON_FALSE;
}


int
neon_mastermind_healthcheck()
{
    // not in service
    if(mastermind_current == NULL)
        return 0;
    
    // in service but expired mastermind information
    if(neon_mastermind_expired() == NEON_TRUE)
        return 1;
    
    // in service
    return 2;
}

