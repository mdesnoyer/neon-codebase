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
#include "neon_mastermind.h"
#include "neonException.h"
#include "mastermind.h"


static Mastermind * mastermind_current = 0;
static Mastermind * mastermind_old = 0;

char * neon_load_error = 0;
const int neon_load_error_size = 2048;

Mastermind *
neon_get_mastermind(){

    return  mastermind_current;
}


static NEON_BOOLEAN
deallocate_mastermind(Mastermind * m){
    if(m == 0)
        return NEON_TRUE;
    
    m->Shutdown();
    
    delete m;
    return NEON_TRUE;
}


NEON_BOOLEAN
neon_mastermind_init(){
    
    neon_load_error = new char[neon_load_error_size + 1];
    
    mastermind_current = new Mastermind();
    mastermind_current->Init();
    
    mastermind_old = new Mastermind();
    mastermind_old->Init();

    return NEON_TRUE;
}


void 
neon_mastermind_shutdown() {
    deallocate_mastermind(mastermind_current);
    deallocate_mastermind(mastermind_old);
    mastermind_current = 0;
    mastermind_old = 0;
    
    if(neon_load_error)
        delete neon_load_error;
}


NEON_LOAD_ERROR
neon_mastermind_load(const char * filepath){

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
        //NeonLog::Error("Neon mastermind load complete");

    }
    catch (NeonException * error)
    {
        // create error message
        snprintf(neon_load_error, neon_load_error_size, "%s", error->GetMessage());
        delete error;
        
        // erase candidate
        if(candidate)
            delete candidate;
        
        return NEON_LOAD_FAIL;
    }
    catch (std::bad_alloc e) {
        
        snprintf(neon_load_error, neon_load_error_size, "%s", "unable to allocate memory");
        neon_stats[NGINX_OUT_OF_MEMORY]++; 
        
        // erase candidate
        if(candidate)
            delete candidate;
        
        return NEON_LOAD_FAIL;
    }
    catch (...) {
        
        snprintf(neon_load_error, neon_load_error_size, "%s", "unable to allocate memory");
        
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
                                  int * account_id_size){

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
                                    ngx_str_t * bucketId,
                                    int height,
                                    int width,
                                    const char ** url,
                                    int * size){
    
    Mastermind * mastermind = neon_get_mastermind();
    
    if(mastermind_current == 0)
        return NEON_MASTERMIND_IMAGE_URL_LOOKUP_FAIL;

    
    (*url) = mastermind->GetImageUrl(accountId, videoId, 
                                      bucketId->data, bucketId->len,
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
                            ngx_str_t * bucketId,
                            const char ** tid,
                            int * size){

    Mastermind * mastermind = neon_get_mastermind();
    
    if(mastermind_current == 0)
        return NEON_MASTERMIND_TID_LOOKUP_FAIL;

    
    (*tid) = mastermind->GetThumbnailID(accountId, videoId, 
                                        bucketId->data, 
                                        bucketId->len, 
                                        *size); 

    
    if(*tid == 0)
        return NEON_MASTERMIND_TID_LOOKUP_NOT_FOUND;
    

    return NEON_MASTERMIND_TID_LOOKUP_OK;
}

/*
 * Check if the current mastermind data has expired
 *
 * */

NEON_BOOLEAN
neon_mastermind_expired(){
    
    if(mastermind_current == NULL)
        return NEON_TRUE;
    
    time_t now = time(0);
    
    if(now >= mastermind_current->GetExpiry())
        return NEON_TRUE;
    
    return NEON_FALSE;
}

/*
 * Health Check handler
 *
 * This is used by the ISP nginx module hook to 
 * determine the state of the Image Platform
 *
 * */

int
neon_mastermind_healthcheck()
{
    // not in service
    if(mastermind_current == NULL)
        return 0;
    
    // in service but expired mastermind information
    if(neon_mastermind_expired() == NEON_TRUE){
        //NeonLog::Error("mastermind expired: %d", mastermind_current->GetExpiry());
        return 1;
    }
    
    // in service
    return 2;
}

NEON_BOOLEAN 
neon_mastermind_is_expiry_greater_than_current(time_t exp){
    if(exp >= mastermind_current->GetExpiry())
        return NEON_TRUE;
    else
        return NEON_FALSE;
}
