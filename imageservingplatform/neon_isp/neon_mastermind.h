#ifndef _NEON_MASTERMIND_C
#define _NEON_MASTERMIND_C

#include <iostream>

#ifdef __cplusplus
extern "C" {
#endif

#include <time.h>
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "neon_error_codes.h"

/*
 *  Contains an error message in case of function call failure
 */
extern char * neon_mastermind_error;

/*
 *  Initialization
 */
NEON_BOOLEAN neon_mastermind_init();


void
neon_mastermind_shutdown();


/*
 * Check if expiry is greater than current mastermind
 * */

NEON_BOOLEAN neon_mastermind_is_expiry_greater_than_current(time_t);

/*
 *  Parse new mastermind file into memory and make it current
 */
typedef enum  {
    
    // all entries successfully parsed and validated
    NEON_LOAD_OK = 0, 
    // while serviceable, some entries in mastermind were rejected 
    NEON_LOAD_PARTIAL,
    // the load failed entirely
    NEON_LOAD_FAIL
    
} NEON_LOAD_ERROR;


NEON_LOAD_ERROR neon_mastermind_load(const char * filepath);

typedef enum  {
    NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_OK = 0,
    NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_FAIL,
    NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_NOT_FOUND
    
} NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_ERROR;
    

    
NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_ERROR
neon_mastermind_account_id_lookup(const char * publisher_id,
                                  const char ** account_id,
                                  int * account_id_size);

    
    

typedef enum  {
    NEON_MASTERMIND_IMAGE_URL_LOOKUP_OK = 0,
    NEON_MASTERMIND_IMAGE_URL_LOOKUP_FAIL,
    NEON_MASTERMIND_IMAGE_URL_LOOKUP_NOT_FOUND
    
} NEON_MASTERMIND_IMAGE_URL_LOOKUP_ERROR;
    
void
neon_mastermind_image_url_lookup(const char * accountId,
                                 const char * videoId,
                                 ngx_str_t * ipAddress,
                                 int height,
                                 int width, 
                                 std::string & image_url);
    
typedef enum  {
    NEON_MASTERMIND_TID_LOOKUP_OK = 0,
    NEON_MASTERMIND_TID_LOOKUP_FAIL,
    NEON_MASTERMIND_TID_LOOKUP_NOT_FOUND
    
} NEON_MASTERMIND_TID_LOOKUP_ERROR;

void
neon_mastermind_tid_lookup(const char * accountId,
                            const char * videoId,
                            ngx_str_t * bucketId, 
                            std::string & thumbnailId);
     
bool
neon_mastermind_find_directive(const char * account_id, 
                               const char * video_id); 
/*
 * Check if current mastermind has expired
 */
NEON_BOOLEAN neon_mastermind_expired();

/*
 *  Get health check status
 *
 *  returns:
 *  0 = not in service
 *  1 = in service but mastermind is passed expiry
 *  2 = in service, mastermind current
 *
 */
int neon_mastermind_healthcheck();
    
#ifdef __cplusplus
} // extern "C"
#endif

#endif

