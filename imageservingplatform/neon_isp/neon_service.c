/* 
 * Neon Service
 * Actual work to be done on the service calls are defined here  
*/

#include <string.h>
#include <errno.h>

#include "neon_constants.h"
#include "neon_log.h"
#include "neon_mastermind.h"
#include "neon_service.h"
#include "neon_stats.h"
#include "neon_utils.h"
#include "neon_service_helper.c"

#define ngx_uchar_to_string(str)     { strlen((const char*)str), (u_char *) str }

/// String Constants used by Neon Service 
static ngx_str_t neon_cookie_name = ngx_string("neonglobaluserid");
//static ngx_str_t cookie_root_domain = ngx_string("; Domain=.neon-lab.com; Path=/;"); 
//static ngx_str_t cookie_neon_domain_prefix = ngx_string("; Domain=.neon-lab.com; Path=");
static ngx_str_t cookie_root_domain = ngx_string("; Domain=.neon-images.com; Path=/;"); 
static ngx_str_t cookie_neon_domain_prefix = ngx_string("; Domain=.neon-images.com; Path=");
static ngx_str_t cookie_max_expiry = ngx_string( "; expires=Thu, 31-Dec-37 23:59:59 GMT"); //expires 2038
static ngx_str_t cookie_expiry_str = ngx_string("; expires=");
static ngx_str_t cookie_client_api = ngx_string("/v1/client/");
static ngx_str_t cookie_semi_colon = ngx_string(";");
static ngx_str_t cookie_fwd_slash = ngx_string("/");
//static char * cloudinary_formatter = "http://res.cloudinary.com/neon-labs/image/upload/w_%d,h_%d/%s.jpg"

/* 
 * Get a particular URI token relative to the base url 
 *
 * */

static unsigned char *
neon_service_get_uri_token(ngx_http_request_t *req, 
                            ngx_str_t * base_url, 
                            int token_index){
    
    // make a null terminated string to use with strtok_r
    size_t uri_size = (req->uri).len + 1;
    unsigned char * uri = (unsigned char*) ngx_pcalloc(req->pool, uri_size);
    if(uri == NULL){
        neon_stats[NGINX_OUT_OF_MEMORY] ++;
        return NULL;
    }
    memset(uri, 0 , uri_size);
    memcpy((char*)uri, (char*)(req->uri).data, (size_t)(req->uri).len);
    
    // move up in the uri when the first token shoud be
    unsigned char * str = uri + base_url->len;
    
    int t = 0;
    char * context = 0;
    char * found_token = 0;
    
    // iterate though all tokens till the one we seek
    for(; t <= token_index; t++) {
        
        // on initial call, provide str pointer
        if(t==0)
            found_token = strtok_r((char*)str, "/?", &context);
        else
            found_token = strtok_r(NULL, "/?", &context);
        
        // should not get this if token is present in uri
        if(found_token == NULL){
            neon_stats[NEON_SERVICE_TOKEN_FAIL] ++;
            return NULL;
        }
            
        // this is the token we're looking for
        if(t==token_index) {
        
            // allocate result token, uri len is a safe size
            size_t token_size = (req->uri).len + 1;
            unsigned char * token = (unsigned char*) ngx_pcalloc(req->pool, 
                                                                 token_size);
            if(token == NULL){
                neon_stats[NGINX_OUT_OF_MEMORY] ++;
                return NULL;
            }
            memset(token, 0 , token_size);
            
            size_t found_size = strlen(found_token);
            strncpy((char*)token, found_token, found_size);
            return token;
        }
        
    }

    // not found
    neon_stats[NEON_SERVICE_TOKEN_NOT_FOUND] ++;
    return NULL;
}

//////////////////// Cookie Helper methods ////////////////////////////

/*
 * TODO: Complete this logic
 * Get bucket id; dummy function now
 *
 * BucketID -> TID Mapping to be defined
 * */


static void 
neon_service_get_bucket_id(ngx_str_t *neon_uuid, 
                            ngx_str_t *video_id, 
                            ngx_str_t * bucket_id){

    unsigned char hashstring[256]; // max size = 18 + sizeof(vid)
    int offset = 0;
    memcpy(hashstring + offset, neon_uuid->data, neon_uuid->len);
    offset += neon_uuid->len;
    if(offset + video_id->len < 256){
        memcpy(hashstring + offset, video_id->data, video_id->len);
        offset += video_id->len;
    }
    
    unsigned long bucket_hash = neon_sdbm_hash(hashstring, offset);
    bucket_hash %= N_ABTEST_BUCKETS;
    char bid[N_ABTEST_BUCKET_DIGITS] = {0}; 
    sprintf(bid, "%ld", bucket_hash);
    bucket_id->data = (unsigned char*) strdup(bid);
    bucket_id->len = strlen(bid);
}

/*
 * Check the presence of a cookie given the cookie key string
 * Also set the value of the cookie
 * 
 * */

static NEON_BOOLEAN 
neon_service_isset_cookie(ngx_http_request_t *request, 
                            ngx_str_t *key, 
                            ngx_str_t *value){

    if (ngx_http_parse_multi_header_lines(&request->headers_in.cookies,
                key, value) == NGX_DECLINED) {
        return NEON_FALSE;
    }

    return NEON_TRUE;
}

/*
 * Method to get the Neon cookie from the request
 * 
 * @return Boolean 
 * */
static NEON_BOOLEAN 
neon_service_isset_neon_cookie(ngx_http_request_t *request){
    ngx_str_t value;
    NEON_BOOLEAN ret = neon_service_isset_cookie(request, 
                                                 &neon_cookie_name, 
                                                 &value);
    if(ret == NEON_FALSE)
        neon_stats[NEON_SERVICE_COOKIE_PRESENT] ++;
    return ret;
}

/*
 * Set the Neon Cookie with Neon UUID 
 *
 * Call this method if the cookie isn't present already
 * 
 * @return: Neon Boolean
 * */

static NEON_BOOLEAN 
neon_service_set_custom_cookie(ngx_http_request_t *request, 
                                ngx_str_t * neon_cookie_name, 
                                ngx_str_t * expires, 
                                ngx_str_t * domain, 
                                char * value, 
                                int value_len){
    
    //http://forum.nginx.org/read.php?2,169118,169118#msg-169118

    static ngx_str_t equal_sign = ngx_string("=");
    u_char *cookie, *p = 0;
    ngx_table_elt_t *set_cookie;
    size_t c_len = 0;

    // Allocate cookie
    c_len = neon_cookie_name->len + value_len + expires->len + domain->len + equal_sign.len; 
    cookie = ngx_pnalloc(request->pool, c_len);
    if (cookie == NULL) {
        //neon_log_error("Failed to allocate memory in the pool for cookie");
        neon_stats[NGINX_OUT_OF_MEMORY] ++;
        return NEON_FALSE;
    }

    p = ngx_copy(cookie, neon_cookie_name->data, neon_cookie_name->len);
    p = ngx_copy(p, equal_sign.data, equal_sign.len);
    p = ngx_copy(p, value, value_len);
    p = ngx_copy(p, expires->data, expires->len);
    p = ngx_copy(p, domain->data, domain->len);

    // Add cookie to the headers list
    set_cookie = ngx_list_push(&request->headers_out.headers);
    if (set_cookie == NULL) {
        neon_stats[NEON_SERVICE_COOKIE_SET_FAIL] ++;
        return NEON_FALSE;
    }

    //Add to the table entry
    set_cookie->hash = 1;
    ngx_str_set(&set_cookie->key, "Set-Cookie");
    set_cookie->value.len = p - cookie;
    set_cookie->value.data = cookie;

    return NEON_TRUE;    
}

/*
 * Set Neon userId cookie with infinite expiry
 * with root path
 *
 * The userid cookie is generated as follows
 * {Random 8 chars}{first 8 digits of timestamp} 
 *
 * The timestamp part of the cookie is used while setting the bucket
 * id cookie for videos. It is used to delay the start of the AB testing.
 * Since the AB Test bucket is based on the hash of user id & video id, 
 * this prevents the race condition in the browser where the AB test bucket
 * cookie gets assigned from an old cookie which gets overwritten by a delayed
 * initial request with no user id cookie. 
 * 
 * */

static NEON_BOOLEAN 
neon_service_set_neon_cookie(ngx_http_request_t *request){

    char neon_id[NEON_UUID_LEN] = {0};
    char timestamp[NEON_UUID_TS_LEN];
    sprintf(timestamp, "%u", (unsigned)time(NULL));

    // Get Neon ID
    neon_get_uuid((char*)neon_id, (size_t)NEON_UUID_RAND_LEN);

    // Add timestamp part to the UUID
    ngx_memcpy(neon_id + NEON_UUID_RAND_LEN, timestamp, NEON_UUID_TS_LEN);

    return neon_service_set_custom_cookie(request, &neon_cookie_name, 
                        &cookie_max_expiry, &cookie_root_domain, neon_id, (size_t)NEON_UUID_LEN);

}

/*
 * Determine if the user is ready start AB Testing
 *
 * If the userid is present, set the uuid param
 *
 * */
static NEON_BOOLEAN
neon_service_userid_abtest_ready(ngx_http_request_t *request, ngx_str_t *uuid){ 

    unsigned int cur_timestamp = (unsigned int) time(NULL);
    
    // check for the neonglobaluserid cookie
    if (neon_service_isset_cookie(request, &neon_cookie_name, uuid) == NEON_TRUE){
       
        char ts[NEON_UUID_TS_LEN];
        ngx_memcpy(ts, uuid->data + NEON_UUID_RAND_LEN, NEON_UUID_TS_LEN);
        unsigned int cookie_ts = atoi((const char*)ts);
        if (cur_timestamp >= cookie_ts + 120)
            return NEON_TRUE;
    }
    
    return NEON_FALSE;
}

/*
 * Set the AB test bucket cookie
 *
 * The AB test cookie is not set in the following cases :
 * 1. The ts part of the neonglobaluserid is < 100secs 
 * 2. Skip setting the cookie if the cookie is already set
 *
 * TODO: In future may be invalidate the old cookie, if the AB test bucket
 * needs to be reset fast. Currently its not required since the expiry on cookie
 * is 10 mins
 *
 * */

static NEON_BOOLEAN
neon_service_set_abtest_bucket_cookie(ngx_http_request_t *request, 
                                        ngx_str_t *video_id, 
                                        ngx_str_t *pub_id,
                                        ngx_str_t *bucket_id){ 

    ngx_str_t c_prefix = ngx_string("neonimg_");
    ngx_str_t underscore = ngx_string("_");
    ngx_str_t expires, domain;
    u_char *p = 0, *dp = 0, *cp = 0;
    time_t add_expiry = 10 *60; //10 mins
   
    // Format the cookie name for bucket id : neonimg_{pub}_{vid}
    ngx_str_t cookie_name;
    int cookie_name_len = c_prefix.len + pub_id->len + 1 + video_id->len;
    cookie_name.data = (u_char *) ngx_palloc(request->pool, cookie_name_len);
    cp = ngx_cpymem(cookie_name.data, c_prefix.data, c_prefix.len); 
    cp = ngx_cpymem(cp, pub_id->data, pub_id->len);
    cp = ngx_cpymem(cp, underscore.data, underscore.len);
    cp = ngx_cpymem(cp, video_id->data, video_id->len);
    cookie_name.len = cp - cookie_name.data;
    
    ngx_str_t value;
    ngx_str_t neonglobaluserid;
    
    // Skip setting the cookie if the ABTest bucket cookie is present
    // Or if the userid isnt' ready for AB Testing !
    if (neon_service_isset_cookie(request, &cookie_name, &value) == NEON_TRUE || 
            neon_service_userid_abtest_ready(request, &neonglobaluserid) == NEON_FALSE){
        return NEON_TRUE; // skip 
    }

    // Bucket ID
    neon_service_get_bucket_id(&neonglobaluserid, video_id, bucket_id); 

    
    // alloc memory, use cookie_max_expiry as a template
    expires.data = (u_char *) ngx_palloc(request->pool, cookie_max_expiry.len);
    p = ngx_cpymem(expires.data, cookie_expiry_str.data, cookie_expiry_str.len); 
    p = ngx_http_cookie_time(p, ngx_time() + add_expiry);
    expires.len = p - expires.data;

    // set cookie path with prefix /v1/client/{PUB}/{VID}
    int d_len = cookie_neon_domain_prefix.len + cookie_client_api.len +  pub_id->len \
                + cookie_fwd_slash.len + video_id->len + cookie_semi_colon.len;
    domain.data = (u_char *) ngx_palloc(request->pool, d_len);
    dp = ngx_cpymem(domain.data, cookie_neon_domain_prefix.data, cookie_neon_domain_prefix.len);
    dp = ngx_cpymem(dp, cookie_client_api.data, cookie_client_api.len);
    dp = ngx_cpymem(dp, pub_id->data, pub_id->len);
    dp = ngx_cpymem(dp, cookie_fwd_slash.data, cookie_fwd_slash.len);
    dp = ngx_cpymem(dp, video_id->data, video_id->len);
    dp = ngx_cpymem(dp, cookie_semi_colon.data, cookie_semi_colon.len);
    domain.len = dp - domain.data;

    return neon_service_set_custom_cookie(request, &cookie_name, 
                        &expires, &domain, (char *)bucket_id->data, bucket_id->len);
}


/*
 * Helper method to 
 * 1. parse the following arguments i) pub id ii) video_id from REST URL
 * 2. Maps publisher id to account id 
 * 3. Extracts IP Address from X-Forwarded-For header or from cip argument 
 * */

static 
int neon_service_parse_api_args(ngx_http_request_t *request, 
                                ngx_str_t *base_url, 
                                const char ** account_id, 
                                int * account_id_size, 
                                unsigned char ** video_id, 
                                unsigned char **publisher_id, 
                                ngx_str_t * ipAddress, 
                                int *width, 
                                int *height,
                                int cleanup_video){

    static const ngx_str_t height_key = ngx_string("height");
    static const ngx_str_t width_key = ngx_string("width");
   
    // get publisher id
    *publisher_id = neon_service_get_uri_token(request, base_url, 0);

    // get video id
    *video_id = neon_service_get_uri_token(request, base_url, 1);
  
    // Clean up the video id from the neonvid_ parameter
    // neonvid_ is a prefix used to identify a Neon video in beacon api
    // Used only for the client API call
    if (cleanup_video == 1)
        neon_service_cleanup_video_id(video_id);

    // get height and width
    ngx_str_t value = ngx_string("");
    *height = 0;
    *width = 0;
    
    ngx_http_arg(request, height_key.data, height_key.len, &value);
    *height = neon_service_parse_number(&value);
   
    ngx_str_t w_value = ngx_string("");
    ngx_http_arg(request, width_key.data, width_key.len, &w_value);
    *width = neon_service_parse_number(&w_value);
  
    // If height && width == -1, i.e if weren't specified then serve
    // default url

    ngx_str_t cip_key = ngx_string("cip");
    ngx_http_arg(request, cip_key.data, cip_key.len, ipAddress);
    
    //static ngx_str_t xf = ngx_string("X-Client-IP");
    static ngx_str_t xf = ngx_string("X-Forwarded-For");
    ngx_table_elt_t * xf_header;
    
    // Check if CIP argument is present, else look for the header
    // Validate the IPAddress string

    if(ipAddress->len == 0 || ipAddress->len > 15){
        xf_header = search_headers_in(request, xf.data, xf.len); 
        if (xf_header && neon_is_valid_ip_string(xf_header->value.data)){
            *ipAddress = xf_header->value;
        }
    }
    
    NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_ERROR error_account_id =
        neon_mastermind_account_id_lookup((char*) *publisher_id,
                                          account_id,
                                          account_id_size);
        
    if(error_account_id != NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_OK) {
        neon_stats[NEON_SERVER_API_ACCOUNT_ID_NOT_FOUND] ++;    
        return 1;
    }
    
    return 0;
} 

/*
 * Format the not found video response for server api call
 *
 * */

static void
neon_service_server_api_not_found(ngx_http_request_t *request,
                                    ngx_chain_t  * chain){
    
    static unsigned char error_response_body[] = "{\"error\":\"thumbnail for video id not found\"}";
    
    ngx_buf_t * b;
    b = (ngx_buf_t *) ngx_pcalloc(request->pool, sizeof(ngx_buf_t));
    if(b == NULL){
        neon_stats[NGINX_OUT_OF_MEMORY] ++;
        return;
    }   
    chain->buf = b;
    chain->next = NULL;
    
    request->headers_out.status = NGX_HTTP_BAD_REQUEST; // 400
    request->headers_out.content_type.len = sizeof("application/json") - 1;
    request->headers_out.content_type.data = (u_char *) "application/json";
    request->headers_out.content_length_n = strlen((char*)error_response_body);
    b->pos = error_response_body;
    b->last = error_response_body + sizeof(error_response_body) -1;
    b->memory = 1; // makes nginx output the buffer as it is
    b->last_buf = 1;
}

/*
 * Format response when image is found for server API
 *
 * */

static void
neon_service_server_api_img_url_found(ngx_http_request_t *request,
                                        ngx_chain_t  * chain,
                                        char * url,
                                        int url_len){

    static ngx_str_t response_body_start = ngx_string("{\"data\":\"");
    static ngx_str_t response_body_end = ngx_string("\",\"error\":\"\"}");
       
    u_char * response_body = 0, *p = 0;
    int response_body_len = 0;
    response_body_len = response_body_start.len + response_body_end.len + url_len;
    response_body = ngx_pnalloc(request->pool, response_body_len);
    if (response_body == NULL){
        request->headers_out.status = NGX_HTTP_INTERNAL_SERVER_ERROR; //500
        neon_stats[NGINX_OUT_OF_MEMORY] ++;
        return;
    }
    p = ngx_copy(response_body, 
            response_body_start.data, 
            response_body_start.len);
    p = ngx_copy(p, url, url_len);
    p = ngx_copy(p, response_body_end.data, response_body_end.len);
 
    ngx_buf_t * b;
    b = (ngx_buf_t *) ngx_pcalloc(request->pool, sizeof(ngx_buf_t));
    if(b == NULL){
        request->headers_out.status = NGX_HTTP_INTERNAL_SERVER_ERROR; //500
        neon_stats[NGINX_OUT_OF_MEMORY] ++;
        return;
    } 
    chain->buf = b;
    chain->next = NULL;
    
    request->headers_out.status = NGX_HTTP_OK;
    request->headers_out.content_type.len = sizeof("application/json") - 1;
    request->headers_out.content_type.data = (u_char *) "application/json";
    request->headers_out.content_length_n = p - response_body;
    b->pos = response_body;
    b->last = p; 
    b->memory = 1;
    b->last_buf = 1;
}

/*
 * Server API Handler 
 *
 * */

NEON_SERVER_API_ERROR
neon_service_server_api(ngx_http_request_t *request,
                         ngx_chain_t  * chain){

    ngx_buf_t * b;
    b = ngx_pcalloc(request->pool, sizeof(ngx_buf_t));
    if(b == NULL){
        request->headers_out.status = NGX_HTTP_INTERNAL_SERVER_ERROR; //500
        neon_stats[NGINX_OUT_OF_MEMORY] ++;
        return NEON_SERVER_API_FAIL;
    } 
    
    chain->buf = b;
    chain->next = NULL;
    
    ngx_str_t base_url = ngx_string("/v1/server/");

    const char * account_id = 0;
    unsigned char * video_id = 0;
    unsigned char * pub_id = 0;
    int account_id_size;
    ngx_str_t ipAddress = ngx_string("");
    int width;
    int height;

    int ret = neon_service_parse_api_args(request, &base_url, &account_id, 
                                           &account_id_size, &video_id, &pub_id, 
                                           &ipAddress, &width, &height, 0);

    if(ret !=0){
        neon_stats[NEON_SERVER_API_ACCOUNT_ID_NOT_FOUND] ++;    
        neon_service_server_api_not_found(request, chain);
        return NEON_SERVER_API_FAIL;
    }
    
    // Send no content if account id is not found or
    // width or height is missing
    // The API spec needs both width & height to be sent  
    if ((width == -1 && height != -1) || (height == -1 && width != -1)){
        //neon_stats[NEON_SERVER_API_INVALID_ARGS] ++;
        neon_service_server_api_not_found(request, chain);
        return NEON_SERVER_API_FAIL;
    }
    
    // look up thumbnail image url
    
    //dummy bucket id, server api doesn't use bucket id currently 
    ngx_str_t bucket_id = ngx_string(""); 
    
    const char * url = 0;
    int url_size = 0;
    
    NEON_MASTERMIND_IMAGE_URL_LOOKUP_ERROR error_url =
        neon_mastermind_image_url_lookup(account_id,
                (char*)video_id,
                &bucket_id,
                height,
                width,
                &url,
                &url_size);
    
    if(error_url != NEON_MASTERMIND_IMAGE_URL_LOOKUP_OK) {
        ngx_log_error(NGX_LOG_ERR, request->connection->log, 0, "IM URL Not Found");
        neon_stats[NEON_SERVER_API_URL_NOT_FOUND] ++;
        neon_service_server_api_not_found(request, chain);
        return NEON_SERVER_API_FAIL;
    }

    neon_service_server_api_img_url_found(request, chain, (char *)url, url_size); 
    return NEON_SERVER_API_OK;
}

/////////// CLIENT API METHODS ////////////

static void
neon_service_no_content(ngx_http_request_t *request,
                                  ngx_chain_t  * chain){

    ngx_buf_t * b;
    b = (ngx_buf_t *) ngx_pcalloc(request->pool, sizeof(ngx_buf_t));
    if(b == NULL){
        request->headers_out.status = NGX_HTTP_INTERNAL_SERVER_ERROR; //500
        neon_stats[NGINX_OUT_OF_MEMORY] ++;
        return;
    } 
    
    chain->buf = b;
    chain->next = NULL;
    
    request->headers_out.status = NGX_HTTP_NO_CONTENT;  // 204
    request->headers_out.content_type.len = sizeof("text/plain") - 1;
    request->headers_out.content_type.data = (u_char *) "text/plain";
    request->headers_out.content_length_n = 0; 
    b->pos = 0;
    b->last = 0;
    b->memory = 1;
    b->last_buf = 1;
}


/*
 * Package 302 HTTP Response to the client with the location header
 * that contains the CDN Image url
 * */
static void
neon_service_client_api_redirect(ngx_http_request_t *request,
                                    ngx_chain_t  * chain,
                                    const char * url_data,
                                    int url_size){

    static ngx_str_t redirect_response_body = ngx_string("redirect to image");
    static ngx_str_t location_header = ngx_string("Location");

    ngx_buf_t * b;
    b = (ngx_buf_t *) ngx_pcalloc(request->pool, sizeof(ngx_buf_t));
    if(b == NULL){
        request->headers_out.status = NGX_HTTP_INTERNAL_SERVER_ERROR; //500
        neon_stats[NGINX_OUT_OF_MEMORY] ++;
        return;
    } 
    
    chain->buf = b;
    chain->next = NULL;
    
    request->headers_out.status = NGX_HTTP_MOVED_TEMPORARILY;  // 302
    request->headers_out.content_type.len = sizeof("text/plain") - 1;
    request->headers_out.content_type.data = (u_char *) "text/plain";
    
    if(request->headers_out.location == 0){
        request->headers_out.location = (ngx_table_elt_t*) ngx_list_push(
                                            &request->headers_out.headers);
    }

    request->headers_out.location->key.len = location_header.len;
    request->headers_out.location->key.data = location_header.data;
    request->headers_out.location->value.len = url_size;
    request->headers_out.location->value.data = (unsigned char*)url_data;
    request->headers_out.location->hash = 1;
    
    b->pos = redirect_response_body.data;
    b->last = redirect_response_body.data + redirect_response_body.len; 
    b->memory = 1;
    b->last_buf = 1;
}


/*
 * Function that resolves the request which comes from the user's browser
 *
 * input: http request, nginx buffer chain
 *
 * @return: NEON_CLIENT_API_OK or NEON_CLIENT_API_FAIL 
 */

NEON_CLIENT_API_ERROR
neon_service_client_api(ngx_http_request_t *request,
                        ngx_chain_t  * chain){
    
    ngx_buf_t * b;
    
    b = ngx_pcalloc(request->pool, sizeof(ngx_buf_t));
    if(b == NULL){
        request->headers_out.status = NGX_HTTP_INTERNAL_SERVER_ERROR; //500
        neon_stats[NGINX_OUT_OF_MEMORY] ++;
        return NEON_CLIENT_API_FAIL;
    } 
    
    chain->buf = b;
    chain->next = NULL;
    
    ngx_str_t base_url = ngx_string("/v1/client/");
   
    const char * account_id = 0;
    unsigned char * video_id = 0;
    unsigned char * pub_id = 0;
    int account_id_size;
    ngx_str_t ipAddress = ngx_string("");
    int width;
    int height;

    int ret = neon_service_parse_api_args(request, &base_url, &account_id, 
                                           &account_id_size, &video_id, &pub_id,
                                           &ipAddress, &width, &height, 1);
       
    if (ret !=0){
        neon_stats[NEON_CLIENT_API_ACCOUNT_ID_NOT_FOUND] ++;
        neon_service_no_content(request, chain);
        return NEON_CLIENT_API_FAIL;
    }
    
    // Send no content if account id is not found or
    // width or height is missing
    // The API spec needs both width & height to be sent  
    if ((width == -1 && height != -1) || (height == -1 && width != -1)){
        //neon_stats[NEON_CLIENT_API_INVALID_ARGS] ++;
        neon_service_no_content(request, chain);
        return NEON_CLIENT_API_FAIL;
    }

    ngx_str_t vid = ngx_uchar_to_string(video_id);
    ngx_str_t pid = ngx_uchar_to_string(pub_id);
    
    // Check if the cookie is present
    if (neon_service_isset_neon_cookie(request) == NEON_FALSE){
        if(neon_service_set_neon_cookie(request) == NEON_TRUE) {
            // Neonglobaluserid cookie set
            neon_stats[NEON_SERVICE_COOKIE_SET] ++;
        }    
    }
    
    // Set the AB Test bucket cookie
    ngx_str_t bucket_id = ngx_string(""); 
    neon_service_set_abtest_bucket_cookie(request, &vid, &pid, &bucket_id);

    // look up thumbnail image url
    
    const char * url= 0;
    int url_size = 0;

    NEON_MASTERMIND_IMAGE_URL_LOOKUP_ERROR error_url =
        neon_mastermind_image_url_lookup(account_id,
                (char*)video_id,
                &bucket_id,
                height,
                width,
                &url,
                &url_size);

    if(error_url != NEON_MASTERMIND_IMAGE_URL_LOOKUP_OK) {
        neon_stats[NEON_CLIENT_API_URL_NOT_FOUND] ++;
        neon_service_no_content(request, chain);
        return NEON_CLIENT_API_FAIL;
    }

    // set up the response with a redirect
    neon_service_client_api_redirect(request,
            chain,
            url,
            url_size);
    
    return NEON_CLIENT_API_OK;
}

/* Get Thumbnail ID service handler */

NEON_GETTHUMB_API_ERROR 
neon_service_getthumbnailid(ngx_http_request_t *request,
                            ngx_chain_t  * chain){

    int clen = 0; 

    ngx_str_t base_url = ngx_string("/v1/getthumbnailid/");
    ngx_str_t params_key = ngx_string("params");
    ngx_str_t video_ids; 
    ngx_str_t bucket_id = ngx_string(""); 
    ngx_str_t neonglobaluserid;
    NEON_BOOLEAN abtest_ready = NEON_FALSE;
    
    ngx_http_arg(request, params_key.data, params_key.len, &video_ids);
    
    // Check if the user is ready to be in a A/B test bucket
    abtest_ready = neon_service_userid_abtest_ready(request, &neonglobaluserid);
    
    //ngx_str_t ipAddress = ngx_string("");
    //static ngx_str_t xf = ngx_string("X-Forwarded-For");
    //ngx_table_elt_t * xf_header;
    //xf_header = search_headers_in(request, xf.data, xf.len); 

    // Check if X-Forwarded-For header is set (Its set by the load balancer)
    //if (xf_header){
    //    ipAddress = xf_header->value;
    //}

    // get publisher id
    unsigned char * publisher_id = neon_service_get_uri_token(request, &base_url, 0);
    
    // Account ID
    const char * account_id = 0;
    int account_id_size = 0;
    
    NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_ERROR error_account_id =
        neon_mastermind_account_id_lookup((char*)publisher_id,
                                          &account_id,
                                          &account_id_size);
    
    if(error_account_id != NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_OK){
        neon_stats[NEON_CLIENT_API_ACCOUNT_ID_NOT_FOUND] ++;
        // Same response as client api not found
        neon_service_no_content(request, chain);
        return NEON_GETTHUMB_API_FAIL;
    }

    /*
    static ngx_str_t j_open = ngx_string("{");
    static ngx_str_t j_close = ngx_string("}");
    static ngx_str_t quote = ngx_string("\"");
    static ngx_str_t comma = ngx_string(",,");
    static ngx_buf_t comma_buf;
    comma_buf.pos = comma.data;
    comma_buf.last = comma.data + comma.len;
    */

    static ngx_str_t colon = ngx_string(":");
    static ngx_str_t noimage = ngx_string("null");
    // use sprintf

    ngx_buf_t * video_buf; 
    ngx_chain_t * vchain;
    ngx_chain_t * prev = chain; 
   
    //dummy 
    ngx_buf_t * colon_buf = ngx_calloc_buf(request->pool);
    colon_buf->pos = colon.data;
    colon_buf->last = colon.data + colon.len;

    chain->buf = colon_buf; 
    chain->next = NULL;

    const char * tid = 0;
    int tid_size = 0;

    char * context = 0;
    const char s[] = ", \n";
    char *vids = strndup((char *)video_ids.data, video_ids.len);
    char *vtoken = strtok_r(vids, s, &context);
    while(vtoken != NULL){
        size_t sz = strlen(vtoken) +1;
        unsigned char * video_id = ngx_pcalloc(request->pool, sz);
        memset(video_id, 0, sz);
        strncpy((char*) video_id, vtoken, sz);
        
        ngx_str_t vid_str = ngx_uchar_to_string(video_id);

        // Get the bucket id for a given video
        if(abtest_ready == NEON_TRUE){
            neon_service_get_bucket_id(&neonglobaluserid, &vid_str, &bucket_id);
        }

        NEON_MASTERMIND_TID_LOOKUP_ERROR err =
            neon_mastermind_tid_lookup(account_id,
                    (const char*)video_id,
                    &bucket_id,
                    &tid,
                    &tid_size);

        video_buf = ngx_calloc_buf(request->pool);
        video_buf->memory = 1;   
        vchain = ngx_pcalloc(request->pool, sizeof(ngx_chain_t));

        if(err == NEON_MASTERMIND_TID_LOOKUP_OK) {
            video_buf->pos = (unsigned char *)tid;
            video_buf->last = (unsigned char *)tid + tid_size;
            clen += tid_size;
        }else{
            video_buf->pos = noimage.data;
            video_buf->last = noimage.data + noimage.len;
            clen += noimage.len;
        }
        
        //Add VideoId to the buffer chain
        prev->next = vchain;
        vchain->buf = video_buf;
        vchain->next = NULL;
        prev = vchain;
        
        vtoken = strtok_r(NULL, s, &context);
        // check if this is the final token
        if (vtoken){
                // Add seperator buffer
                ngx_chain_t * s_chain = ngx_pcalloc(request->pool, sizeof(ngx_chain_t));
                ngx_buf_t * s_buf = ngx_calloc_buf(request->pool);
                char * sep = ",";
                s_buf->pos = (unsigned char*) sep;
                s_buf->memory = 1;   
                s_buf->last = (unsigned char*) sep + 1; 
                clen += 1;
                
                prev->next = s_chain;
                s_chain->buf = s_buf;
                s_chain->next = NULL; 
                prev = s_chain;
        }
    }
    request->headers_out.status = NGX_HTTP_OK;
    request->headers_out.content_type.len = sizeof("application/json") - 1;
    request->headers_out.content_type.data = (u_char *) "application/json";
    request->headers_out.content_length_n = clen;
    video_buf->last_buf = 1; //Mark the last buffer   
        
    return NEON_GETTHUMB_API_OK;
}

// Getting geoip stuff in nginx
//ngx_str_t variable_name = ngx_string("geoip_country_code");
//    ngx_http_variable_value_t * geoip_country_code_var =
//    ngx_http_get_variable( r, &variable_name, 0);
