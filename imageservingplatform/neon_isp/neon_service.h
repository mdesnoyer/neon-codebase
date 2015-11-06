#ifndef _NEON_SERVICE_
#define _NEON_SERVICE_

#include <boost/scoped_ptr.hpp>

#ifdef __cplusplus
extern "C" {
#endif

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "neon_error_codes.h"

typedef enum  {
    API_OK = 0,
    API_FAIL
} API_ERROR;

API_ERROR neon_service_client_api(ngx_http_request_t *req,
                                  ngx_chain_t  * chain);

API_ERROR neon_service_server_api(ngx_http_request_t *req,
                                  ngx_chain_t  * chain);

API_ERROR neon_service_getthumbnailid(ngx_http_request_t *req,
                                      ngx_chain_t  ** chain);

API_ERROR neon_service_video(ngx_http_request_t *req,
                             ngx_chain_t  * chain);

#ifdef __cplusplus
} // extern "C"
#endif


#endif

