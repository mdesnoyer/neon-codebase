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
#include "neon_service_helper.h"

#define ngx_uchar_to_string(str)     { strlen((const char*)str), (u_char *) str }

/// String Constants used by Neon Service 
static ngx_str_t neon_cookie_name = ngx_string("neonglobaluserid");
static ngx_str_t cookie_root_domain = ngx_string("; Domain=.neon-images.com; Path=/;"); 
static ngx_str_t cookie_neon_domain_prefix = ngx_string("; Domain=.neon-images.com; Path=");
static ngx_str_t cookie_max_expiry = ngx_string( "; expires=Thu, 31-Dec-37 23:59:59 GMT"); //expires 2038
static ngx_str_t cookie_expiry_str = ngx_string("; expires=");
static ngx_str_t cookie_client_api = ngx_string("/v1/client/");
static ngx_str_t cookie_semi_colon = ngx_string(";");
static ngx_str_t cookie_fwd_slash = ngx_string("/");

/* 
 * Get a particular URI token relative to the base url 
 *
 * */

static unsigned char *
neon_service_get_uri_token(const ngx_http_request_t *req, 
                            ngx_str_t * base_url, 
                            int token_index){
    
    // make a null terminated string to use with strtok_r
    size_t uri_size = (req->uri).len + 1;
    unsigned char * uri = (unsigned char*) ngx_pcalloc(req->pool, uri_size);
    if(uri == NULL){
        neon_stats[NGINX_OUT_OF_MEMORY] ++;
        return NULL;
    }
    memset(uri, 0, uri_size);
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
 * Given a unique identifier, VideoId, generate and set the bucket id for 
 * a given video in to bucketId  
 *
 * NOTE: The identifier is a neonglobaluserid in most cases. but when the
 * user is not ready to be A/B tested, the ipAddress of the user is
 * used at the identifier to generate the bucket ID
 *
 * bucketId = hash(identifier + video_id)
 * */

static void 
neon_service_set_bucket_id(const ngx_str_t * identifier, 
                           const ngx_str_t * video_id, 
                           ngx_str_t * bucket_id,
                           ngx_pool_t *pool){

    unsigned char hashstring[256]; // max size = 18 + sizeof(vid)
    memset(hashstring, 0, 256);
    int offset = 0;
    memcpy(hashstring + offset, identifier->data, identifier->len);
    offset += identifier->len;
    if(offset + video_id->len < 256){
        memcpy(hashstring + offset, video_id->data, video_id->len);
        offset += video_id->len;
    }
    
    unsigned long bucket_hash = neon_sdbm_hash(hashstring, offset);
    bucket_hash %= N_ABTEST_BUCKETS;
    bucket_id->data = ngx_pcalloc(pool, N_ABTEST_BUCKET_DIGITS + 1);
    sprintf((char*)bucket_id->data, "%x", (unsigned int)bucket_hash);
    bucket_id->len = strlen((char*)bucket_id->data);
}

/*
 * Check the presence of a cookie given the cookie key string
 * Also set the value of the cookie
 * 
 * */

static NEON_BOOLEAN 
neon_service_isset_cookie(ngx_http_request_t * request, 
                          ngx_str_t * key, 
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
    if(ret == NEON_TRUE)
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
        ngx_log_error(NGX_LOG_ERR, request->connection->log, 
                       0, "Failed to allocate memory in the pool for cookie");
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
 * If the userid is present in the cookie, set the uuid arg 
 *
 * */
static NEON_BOOLEAN
neon_service_userid_abtest_ready(ngx_http_request_t *request, ngx_str_t *uuid){ 

    unsigned int cur_timestamp = (unsigned int) time(NULL);
    
    // check for the neonglobaluserid cookie
    if (neon_service_isset_cookie(request, &neon_cookie_name, uuid) == NEON_TRUE)
    {  
        char ts[NEON_UUID_TS_LEN];
        // TODO: Protect against fake cookie timestamp, or invalid atoi
        // conversion
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
    time_t add_expiry = 10 * 60; //10 mins
   
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
    if (neon_service_isset_cookie(request, &cookie_name, &value) == NEON_TRUE){
        return NEON_TRUE; // skip 
    }
    
    // Or if the userid isnt' ready for AB Testing !
    if (neon_service_userid_abtest_ready(request, &neonglobaluserid) == NEON_FALSE){
        return NEON_TRUE; // skip 
    }

    // Bucket ID
    neon_service_set_bucket_id(&neonglobaluserid, video_id, bucket_id, request->pool); 
    
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
                                int remove_neon_prefix){

    static const ngx_str_t height_key = ngx_string("height");
    static const ngx_str_t width_key = ngx_string("width");
   
    // get publisher id
    *publisher_id = neon_service_get_uri_token(request, base_url, 0);

    if(*publisher_id == NULL) {
        neon_stats[NEON_SERVICE_PUBLISHER_ID_MISSING_FROM_URL]++;     
        return 1;
    }

    // get an allocated video id
    *video_id = neon_service_get_uri_token(request, base_url, 1);
  
    if(*video_id == NULL) {
        neon_stats[NEON_SERVICE_VIDEO_ID_MISSING_FROM_URL]++;                     
        return 1;
    }

    // remove the trailing jpg extention, if any.  
    remove_jpg_extention(*video_id); 

    // Clean up the video id from the neonvid_ parameter
    // neonvid_ is a prefix used to identify a Neon video in beacon api
    // Used only for the client API call
    if (remove_neon_prefix  == 1) {
          const char * prefix = "neonvid_";
          const int prefix_size = 8;
    
          // look for the prefix and skip ahead of it 
          if(ngx_strncmp(*video_id, prefix, prefix_size) == 0) {    
            *video_id = *video_id + prefix_size;
          }
          // no prefix, this request is invalid
          else {
              neon_stats[NEON_SERVICE_VIDEO_ID_MISSING_FROM_URL]++;
              return 1;
          }
    }

    // get height and width
    ngx_str_t value = ngx_string("");
    *height = 0;
    *width = 0;
    
    ngx_http_arg(request, height_key.data, height_key.len, &value);
    *height = neon_service_parse_number(&value);
   
    ngx_str_t w_value = ngx_string("");
    ngx_http_arg(request, width_key.data, width_key.len, &w_value);
    *width = neon_service_parse_number(&w_value);
  
    // If height or width == -1, i.e if weren't specified then serve
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
neon_service_add_to_chain(ngx_http_request_t *request, ngx_chain_t  * chain, ngx_buf_t* buf) 
{ 
    int found_last_buffer = 0; 
    ngx_chain_t * added_link;
    
    for ( ; ; ) {  
       if (chain && chain->buf && chain->buf->last_buf) 
           found_last_buffer = 1;
       if (chain == NULL || chain->next == NULL) 
           break;  
       chain = chain->next; 
    }  

    if (buf) { 
        buf->memory = 1;
        buf->last_buf = 1;  
    } 
    else { 
        return; 
    } 
    if (found_last_buffer) { 
        added_link = ngx_alloc_chain_link(request->pool);
        if (added_link == NULL) {
            request->headers_out.status = NGX_HTTP_INTERNAL_SERVER_ERROR; //500
            neon_stats[NGINX_OUT_OF_MEMORY] ++;
            return;
        }
        added_link->buf = buf;
        added_link->next = NULL;
        chain->next = added_link; 
        chain->buf->last_buf = 0; 
        added_link->buf->last_buf = 1;  
    } 
    else {  
        chain->buf = buf;
        chain->next = NULL; 
    } 
}

static void 
neon_service_set_json_headers(ngx_http_request_t *request, int status, int content_length) 
{ 
    request->headers_out.content_length_n = content_length;
    request->headers_out.status = status;
    request->headers_out.content_type.len = sizeof("application/json") - 1;
    request->headers_out.content_type.data = (u_char *) "application/json";
}

static void 
neon_service_set_redirect_headers(ngx_http_request_t *request, ngx_buf_t *buf) 
{ 
    static ngx_str_t location_header = ngx_string("Location");
    request->headers_out.status = NGX_HTTP_MOVED_TEMPORARILY;  // 302
    request->headers_out.content_type.len = sizeof("text/plain") - 1;
    request->headers_out.content_type.data = (u_char *) "text/plain";
    
    if(request->headers_out.location == 0){
        request->headers_out.location = (ngx_table_elt_t*) ngx_list_push(
                                            &request->headers_out.headers);
    }

    request->headers_out.location->key.len = location_header.len;
    request->headers_out.location->key.data = location_header.data;
    request->headers_out.location->value.len = buf->last - buf->pos;
    request->headers_out.location->value.data = (unsigned char*)buf->pos;
    request->headers_out.location->hash = 1;
}

static void
neon_service_set_no_content_headers(ngx_http_request_t *request)
{
    request->headers_out.status = NGX_HTTP_NO_CONTENT;  // 204
    request->headers_out.content_type.len = sizeof("text/plain") - 1;
    request->headers_out.content_type.data = (u_char *) "text/plain";
    request->headers_out.content_length_n = 0; 
}

/*
 * Server API Handler 
 *
 * */

NEON_SERVER_API_ERROR
neon_service_server_api(ngx_http_request_t *request, ngx_chain_t  * chain) 
{

    ngx_buf_t * buf;
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
    int width, height, content_length = 0; 

    int ret = neon_service_parse_api_args(request, &base_url, &account_id, 
                                           &account_id_size, &video_id, &pub_id, 
                                           &ipAddress, &width, &height, 0);

    // Send no content if account id is not found 
    if(ret !=0){
        neon_stats[NEON_SERVER_API_ACCOUNT_ID_NOT_FOUND] ++;    
        neon_service_server_api_not_found(request, chain);
        return NEON_SERVER_API_FAIL;
    }
    
    
    //dummy bucket id, server api doesn't use bucket id currently 
    ngx_str_t bucket_id = ngx_string("");

    buf = ngx_calloc_buf(request->pool);
    ngx_str_t start = ngx_string("{\"data\":\"");
    buf->pos = start.data; 
    buf->last = buf->pos + start.len; 
    content_length += start.len;  
    neon_service_add_to_chain(request, chain, buf); 
    
    // look up thumbnail image url
    char *url = NULL; 
    buf = ngx_calloc_buf(request->pool);
    NEON_MASTERMIND_IMAGE_URL_LOOKUP_ERROR error_url =
        neon_mastermind_image_url_lookup(account_id,
                (char*)video_id,
                &bucket_id,
                height,
                width,
                &url);
    
    if(error_url != NEON_MASTERMIND_IMAGE_URL_LOOKUP_OK) {
        ngx_log_error(NGX_LOG_ERR, request->connection->log, 0, "IM URL Not Found");
        neon_stats[NEON_SERVER_API_URL_NOT_FOUND] ++;
        neon_service_server_api_not_found(request, chain);
        return NEON_SERVER_API_FAIL;
    }

    buf->pos = (u_char*)url; 
    buf->last = buf->pos + strlen(url); 
    content_length += strlen(url);  

    neon_service_add_to_chain(request, chain, buf);

    // Temp solution, figure out a way to use the nginx logger
    if (strstr(url,"cloudinary") != NULL) {
        ngx_log_error(NGX_LOG_ERR, request->connection->log, 0, 
                        "Cloudinary URL generated for video %s h %d w %d", 
                        video_id, height, width);
    }  

    ngx_str_t end = ngx_string("\",\"error\":\"\"}");
    buf = ngx_calloc_buf(request->pool);
    buf->pos = end.data; 
    buf->last = buf->pos + end.len;  
    content_length += end.len;  
    neon_service_add_to_chain(request, chain, buf);
    
    // set the headers and the request is done! 
    neon_service_set_json_headers(request, NGX_HTTP_OK, content_length); 

    return NEON_SERVER_API_OK;
}

/////////// CLIENT API METHODS ////////////

/*
 * Function that resolves the request which comes from the user's browser
 *
 * input: http request, nginx buffer chain
 *
 * Code flow 
 * - parse all the args from URI & ip address
 * - check if Neon UUID cookie is present, if not set it
 * - set A/B test bucket cookie for the given video
 * - Lookup the image for the given videoId & bucketId
 * - send redirect response to the user
 *
 * @return: NEON_CLIENT_API_OK or NEON_CLIENT_API_FAIL 
 */

NEON_CLIENT_API_ERROR
neon_service_client_api(ngx_http_request_t *request,
                        ngx_chain_t  * chain){

    ngx_str_t base_url = ngx_string("/v1/client/");
    ngx_buf_t *buf; 
   
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
        neon_service_set_no_content_headers(request);
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
    
    // Check if the user is ready for A/B Testing, if no then use the ip adress to
    // generate the bucketId 
    ngx_str_t neonglobaluserid = ngx_string("");
    if (neon_service_userid_abtest_ready(request, &neonglobaluserid) == NEON_FALSE){
        neon_service_set_bucket_id(&ipAddress, &vid, &bucket_id, request->pool);
    }

    // look up thumbnail image url
    buf = ngx_calloc_buf(request->pool);
    ngx_str_t redirect_str = ngx_string("redirect to image");
    buf->pos = redirect_str.data; 
    buf->last = buf->pos + redirect_str.len;  
    neon_service_add_to_chain(request, chain, buf); 
 
    char *url = NULL; 
    buf = ngx_calloc_buf(request->pool);
    NEON_MASTERMIND_IMAGE_URL_LOOKUP_ERROR error_url =
        neon_mastermind_image_url_lookup(account_id,
                (char*)video_id,
                &bucket_id,
                height,
                width,
                &url);

    if(error_url != NEON_MASTERMIND_IMAGE_URL_LOOKUP_OK) {
        neon_stats[NEON_CLIENT_API_URL_NOT_FOUND] ++;
        neon_service_set_no_content_headers(request);
        return NEON_CLIENT_API_FAIL;
    }

    buf->pos = (u_char*)url; 
    buf->last = buf->pos + strlen(url); 
    // Temp solution, figure out a way to use the nginx logger
    if (strstr(url,"cloudinary") != NULL) {
        ngx_log_error(NGX_LOG_ERR, request->connection->log, 0, 
                        "Cloudinary URL generated for video %s h %d w %d", 
                        video_id, height, width);
    }  
    // we don't want to add the url to the chain here, since we are 
    // simply redirecting, set the headers with the url information 
    // and off we go. 
    neon_service_set_redirect_headers(request, buf); 
    
    return NEON_CLIENT_API_OK;
}


/* Get Thumbnail ID service handler */
NEON_GETTHUMB_API_ERROR 
neon_service_getthumbnailid(ngx_http_request_t *request, ngx_chain_t  **  chain)
{
    int wants_html = 0, clen = 0; 

    ngx_str_t base_url = ngx_string("/v1/getthumbnailid/");
    ngx_str_t params_key = ngx_string("params");
    ngx_str_t video_ids = ngx_string(""); 
    ngx_str_t bucket_id = ngx_string(""); 
    ngx_str_t neonglobaluserid;
    NEON_BOOLEAN abtest_ready = NEON_FALSE;
    
    ngx_http_arg(request, params_key.data, params_key.len, &video_ids);
    
    // Check if the user is ready to be in a A/B test bucket
    abtest_ready = neon_service_userid_abtest_ready(request, &neonglobaluserid);
    
    // Get IP Address
    static ngx_str_t xf = ngx_string("X-Forwarded-For");
    ngx_str_t ipAddress = ngx_string("");
    ngx_table_elt_t * xf_header;
    if(ipAddress.len == 0 || ipAddress.len > 15){
        xf_header = search_headers_in(request, xf.data, xf.len); 
        if (xf_header && neon_is_valid_ip_string(xf_header->value.data)){
            ipAddress = xf_header->value;
        }
    }

    // get publisher id
    unsigned char * publisher_id = neon_service_get_uri_token(request, &base_url, 0);
    char * token = strtok((char*)publisher_id, "."); 
    if (token) {
        publisher_id = (unsigned char *)token; 
        char * extension = strtok(NULL, "."); 
        if (extension && strcmp(extension, (char*)"html") == 0) 
            wants_html = 1; 
    }  
    if(publisher_id == NULL) {
        neon_stats[NEON_GETTHUMBNAIL_API_PUBLISHER_NOT_FOUND] ++;
        neon_service_set_no_content_headers(request);
        return NEON_GETTHUMB_API_FAIL;
    }
    // Account ID
    const char * account_id = 0;
    int account_id_size = 0;
    
    NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_ERROR error_account_id =
        neon_mastermind_account_id_lookup((char*)publisher_id,
                                          &account_id,
                                          &account_id_size);
    
    if(error_account_id != NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_OK){
        neon_stats[NEON_GETTHUMBNAIL_API_ACCOUNT_ID_NOT_FOUND] ++;
        neon_service_set_no_content_headers(request);
        return NEON_GETTHUMB_API_FAIL;
    }

    static ngx_str_t noimage = ngx_string("null");

    // used repetitively
    ngx_buf_t * buf; 
    char * context = 0;
    const char s[] = ", \n";
    
    // If video_ids haven't been parsed 
    if (video_ids.len <= 0){
        neon_service_set_no_content_headers(request);
        return NEON_GETTHUMB_API_FAIL;
    }

    // make a copy of params o we can parse and extract them with str_tok
    // this could be better with ngx functions
    unsigned char * vids = ngx_pcalloc(request->pool, video_ids.len + 1);
    vids[video_ids.len] = 0;
    strncpy((char*) vids, (char *)video_ids.data, video_ids.len);
    char *vtoken = strtok_r((char*)vids, s, &context);

    (*chain) = (ngx_chain_t*)ngx_pcalloc(request->pool, sizeof(ngx_chain_t));
    
    if (wants_html) { 
        static ngx_str_t response_body_start = ngx_string("<!DOCTYPE html><html><head><script type='text/javascript'>window.parent.postMessage('");
        buf = ngx_calloc_buf(request->pool);
     //   buf->memory = 1;   
    //    *chain = ngx_pcalloc(request->pool, sizeof(ngx_chain_t));
        buf->start = buf->pos  = response_body_start.data;
        buf->end = buf->last = buf->pos + response_body_start.len;
        clen += response_body_start.len;
        neon_service_add_to_chain(request, (*chain), buf); 
      //  (*chain)->buf = buf;
     //   (*chain)->next = NULL;
     //   chain = &(*chain)->next;
    }  
    // for each video id  passd to us as params
    while(vtoken != NULL) {

        size_t sz = strlen(vtoken) +1;
        unsigned char * video_id = ngx_pcalloc(request->pool, sz);
        memset(video_id, 0, sz);
        strncpy((char*) video_id, vtoken, sz);
        
        ngx_str_t vid_str = ngx_uchar_to_string(video_id);

        // Get the bucket id for a given video
        if(abtest_ready == NEON_TRUE){
            neon_service_set_bucket_id(&neonglobaluserid, &vid_str, &bucket_id, request->pool);
        }else{
            // Use the IP Address of the client to generate the bucket_id
            neon_service_set_bucket_id(&ipAddress, &vid_str, &bucket_id, request->pool);
        }

        char *tid = NULL; 
        NEON_MASTERMIND_TID_LOOKUP_ERROR err =
            neon_mastermind_tid_lookup(account_id,
                    (const char*)video_id,
                    &bucket_id,
                    &tid);

        // allocate a buffer and its chain
        buf = ngx_calloc_buf(request->pool);

        if(err == NEON_MASTERMIND_TID_LOOKUP_OK) {
            buf->start = buf->pos  = (u_char*)tid;
            buf->end = buf->last = buf->pos + strlen(tid);
            clen += strlen(tid);
        }else{
            buf->start = buf->pos = noimage.data;
            buf->end = buf->last = noimage.data + noimage.len;
            clen += noimage.len;
        }
        
        // add this chain and lets setup the next
        neon_service_add_to_chain(request, (*chain), buf); 
       
        // let's see if there is another token to process
        vtoken = strtok_r(NULL, s, &context);
        
        // if there's another token, then we need a separator
        if (vtoken){
             // Add separator buffer
             ngx_buf_t * s_buf = ngx_calloc_buf(request->pool);
             s_buf->start = s_buf->pos = (u_char*)",";
             s_buf->end = s_buf->last = s_buf->pos + 1; 
             neon_service_add_to_chain(request, (*chain), s_buf); 
             clen += 1;
        }
    }
    if (wants_html) { 
        static ngx_str_t response_body_end = ngx_string("', '*')</script></head><body></body></html>");
        buf = ngx_calloc_buf(request->pool);
        //buf->memory = 1;   
        //*chain = ngx_pcalloc(request->pool, sizeof(ngx_chain_t));
        buf->start = buf->pos  = response_body_end.data;
        buf->end = buf->last = buf->pos + response_body_end.len;
        clen += response_body_end.len;
        neon_service_add_to_chain(request, (*chain), buf); 
        //(*chain)->buf = buf;
        //(*chain)->next = NULL;
        //chain = &(*chain)->next;
    }  

    request->headers_out.status = NGX_HTTP_OK;
    if (wants_html) { 
        request->headers_out.content_type.len = strlen("text/html");
        request->headers_out.content_type.data = (u_char *) "text/html";
    }
    else { 
        request->headers_out.content_type.len = strlen("text/plain");
        request->headers_out.content_type.data = (u_char *) "text/plain";
    }  
    request->headers_out.content_length_n = clen;
        
    return NEON_GETTHUMB_API_OK;
}
// Getting geoip stuff in nginx
//ngx_str_t variable_name = ngx_string("geoip_country_code");
//    ngx_http_variable_value_t * geoip_country_code_var =
//    ngx_http_get_variable( r, &variable_name, 0);
