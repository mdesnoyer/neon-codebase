#ifndef _NEON_SERVICE_
#define _NEON_SERVICE_

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "neon_error_codes.h"


#ifdef __cplusplus
extern "C" {
#endif

typedef enum  {
    
    NEON_CLIENT_API_OK = 0,
    NEON_CLIENT_API_FAIL
    
} NEON_CLIENT_API_ERROR;

typedef enum  {
    
    NEON_SERVER_API_OK = 0,
    NEON_SERVER_API_FAIL
    
} NEON_SERVER_API_ERROR;

typedef enum  {
    
    NEON_GETTHUMB_API_OK = 0,
    NEON_GETTHUMB_API_FAIL
} NEON_GETTHUMB_API_ERROR;


NEON_CLIENT_API_ERROR neon_service_client_api(ngx_http_request_t *req,
                                                 ngx_chain_t  * chain);

NEON_SERVER_API_ERROR neon_service_server_api(ngx_http_request_t *req,
                                                 ngx_chain_t  * chain);

NEON_GETTHUMB_API_ERROR neon_service_getthumbnailid(ngx_http_request_t *req,
                                                 ngx_chain_t  ** chain);


#ifdef __cplusplus
} // extern "C"
#endif


#endif

