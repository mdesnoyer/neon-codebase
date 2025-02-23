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

char * neon_mastermind_error = 0;
const int neon_mastermind_error_size = 2048;

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
    
    neon_mastermind_error = new char[neon_mastermind_error_size + 1];
    
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
    
    if(neon_mastermind_error)
        delete [] neon_mastermind_error;
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
        
        Mastermind::EINIT_ERRORS err = candidate->Init(filepath, 
                                                       mastermind_current->GetExpiry(),
                                                       neon_mastermind_error,
                                                       neon_mastermind_error_size);
        
        if(err == Mastermind::EINIT_SUCCESS || err == Mastermind::EINIT_PARTIAL_SUCCESS) {
       
            // swap
            mastermind_old = mastermind_current;
            mastermind_current = candidate;

            if(err == Mastermind::EINIT_PARTIAL_SUCCESS)
                return NEON_LOAD_PARTIAL;

            return NEON_LOAD_OK;
        }
    }
    // exception safeguards
    catch (NeonException * error)
    {
        // create error message
        snprintf(neon_mastermind_error, neon_mastermind_error_size, 
            "neon_mastermind_load: neon exception: %s", error->GetMessage());
        delete error;
    }
    catch (std::bad_alloc e) {
        snprintf(neon_mastermind_error, neon_mastermind_error_size, 
            "neon_mastermind_load: %s", "bad  alloc exception");
        neon_stats[NGINX_OUT_OF_MEMORY]++; 
    }
    catch (...) {
        snprintf(neon_mastermind_error, neon_mastermind_error_size, 
            "neon_mastermind_load: %s", "unspecified exception");
    }

    // candidate failed, erase
    if(candidate)
        delete candidate;

    return NEON_LOAD_FAIL;
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


/*********************************************************
 * Name       : neon_mastermind_image_url_lookup 
 * Parameters :  
 *********************************************************/

void 
neon_mastermind_image_url_lookup(const char * accountId,
                                    const char * videoId,
                                    ngx_str_t * bucketId,
                                    int height,
                                    int width, 
                                    std::string & image_url)
{
    Mastermind * mastermind = neon_get_mastermind();
    
    if(mastermind_current == 0) { 
        return;
    } 

    mastermind->GetImageUrl(accountId, videoId, 
                            bucketId->data, bucketId->len,
                            height, width, image_url);
    
    if(image_url.size() == 0) { 
        return; 
    } 
    
    if (image_url.find("cloudinary") != std::string::npos) { 
        ngx_log_debug3(NGX_LOG_INFO, request->connection->log, 0, 
                        "Cloudinary URL generated for video %s h %d w %d", 
                        video_id, height, width); 
    }    
}

//int
bool
neon_mastermind_find_directive(const char * account_id, 
                               const char * video_id) 
{ 
    Mastermind * mastermind = neon_get_mastermind(); 
    
    if(mastermind_current == 0) { 
        return false;
    } 
    
    return mastermind->DoesDirectiveExist(account_id, video_id); 
}


/*
 * TID lookup Interface method 
 *
 * */

void 
neon_mastermind_tid_lookup(const char * accountId,
                            const char * videoId,
                            ngx_str_t * bucketId, 
                            std::string & thumbnailId)
{
    Mastermind * mastermind = neon_get_mastermind();
    
    if(mastermind_current == 0) { 
        return;
    } 

    mastermind->GetThumbnailID(accountId, videoId, 
                                        bucketId->data, 
                                        bucketId->len, 
                                        thumbnailId); 
    if (thumbnailId.size() == 0) { 
        return; 
    }
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
    // no mastermind data structure allocated, typically very early 
    // during server bootup 
    if(mastermind_current == NULL)
        return 0; // no service

    // empty mastermind data structure, typically after server bootup but
    // before a valid mastermind file has been fetched from S3 
    if(mastermind_current->GetExpiry() == 0){
        return 0; // no serivce
    }

    // in service but with expired mastermind information, typically 
    // during mastermind updating process or due to S3 download problems
    if(neon_mastermind_expired() == NEON_TRUE){
        return 1;
    }
    
    // otherwise in service with unexpired mastermind 
    return 2;
}

NEON_BOOLEAN 
neon_mastermind_is_expiry_greater_than_current(time_t exp){
    if(exp > mastermind_current->GetExpiry())
        return NEON_TRUE;
    else
        return NEON_FALSE;
}
