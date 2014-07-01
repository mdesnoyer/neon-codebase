// Neon Service 
// Actual work to be done on the service calls are defined here  

#include <string.h>
#include <errno.h>
#include "neon_log.h"
#include "neon_mastermind.h"
#include "neon_service.h"
#include "neon_stats.h"
#include "neon_utils.h"
#include "neon_service_helper.c"

static unsigned char *
neon_service_get_uri_token(ngx_http_request_t *req, ngx_str_t * base_url, 
                            int token_index)
{
    
    // make a null terminated string to use with strtok_r
    size_t uri_size = (req->uri).len + 1;
    unsigned char * uri = ngx_pcalloc(req->pool, uri_size);
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
            unsigned char * token = ngx_pcalloc(req->pool, token_size);
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


/*
 * Method to get the Neon cookie from the request
 * 
 * @return neon_userid string is filled with the  
 * */
static NEON_BOOLEAN 
neon_service_isset_neon_cookie(ngx_http_request_t *request)
{

    ngx_str_t key = (ngx_str_t)ngx_string("neonglobaluserid");
    ngx_str_t value;
    if (ngx_http_parse_multi_header_lines(&request->headers_in.cookies,
                &key, &value) == NGX_DECLINED) {
            neon_stats[NEON_SERVICE_COOKIE_PRESENT] ++;
        return NEON_FALSE;
    }

    neon_log_error("Neon cookie is %s", value.data);
    return NEON_TRUE;
}

/*
 * Set the Neon Cookie with Neon UUID 
 *
 * Call this method if the cookie isn't present already
 * 
 * @return: Neon Boolean
 * */

static NEON_BOOLEAN 
neon_service_set_custom_cookie(ngx_http_request_t *request, ngx_str_t * neon_cookie_name, 
                    ngx_str_t * expires, ngx_str_t * domain, char * value, int value_len){
    
    //http://forum.nginx.org/read.php?2,169118,169118#msg-169118

    u_char *cookie, *p = 0;
    ngx_table_elt_t *set_cookie;
    size_t c_len = 0;

    // Allocate cookie
    c_len = neon_cookie_name->len + value_len + expires->len + domain->len; 
    cookie = ngx_pnalloc(request->pool, c_len);
    if (cookie == NULL) {
        neon_log_error("Failed to allocate memory in the pool for cookie");
        return NEON_FALSE;
    }

    p = ngx_copy(cookie, neon_cookie_name->data, neon_cookie_name->len);
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

    neon_stats[NEON_SERVICE_COOKIE_SET] ++;
    return NEON_TRUE;    
}

static NEON_BOOLEAN 
neon_service_set_neon_cookie(ngx_http_request_t *request){

    static ngx_str_t neon_cookie_name = ngx_string("neonglobaluserid=");
    static ngx_str_t expires = ngx_string( "; expires=Thu, 31-Dec-37 23:59:59 GMT"); //expires 2038
    static ngx_str_t domain = ngx_string("; Domain=.neon-lab.com; Path=/;"); // should we add HttpOnly?

    char neon_id[16] = {0};
    size_t id_len = 16; //16 char long id
    
    // Get Neon ID
    neon_get_uuid((char*)neon_id, id_len);

    return neon_service_set_custom_cookie(request, &neon_cookie_name, 
                        &expires, &domain, neon_id, id_len);

}


// Function to determine the AB test for the current request
// based on the IP address and the video id requested 
//static int neon_service_get_ab_bucket(ngx_http_request_t *req)
//{
//
//    
//}    

/*
* Generic helper function to build the api response
* Allocates memory from buffer chain and "stiches" it to the request chain
* 
* @input: http_status, content type and response body
*
static void
neon_service_build_api_response(ngx_http_request_t *request,
                                 ngx_chain_t  * chain,
                                 ngx_int_t http_status,
                                 const char * content_type,
                                 int content_type_size,
                                 const char * response_body,
                                 int response_body_size)
{
    ngx_buf_t * b;
    b = (ngx_buf_t *) ngx_pcalloc(request->pool, sizeof(ngx_buf_t));
    
    chain->buf = b;
    chain->next = NULL;
    
    request->headers_out.status = http_status; 
    request->headers_out.content_type.len = content_type_size; 
    request->headers_out.content_type.data = (u_char *) content_type;
    request->headers_out.content_length_n = response_body_size;
    b->pos = (u_char *) response_body;
    b->last = response_body + response_body_size;
    b->memory = 1;
    b->last_buf = 1;

}
*/

/*
 * Helper method to parse arguments (pub id, video_id) the REST API URL
 * Maps publisher id to account id, parses IP Address 
 * */

static 
int neon_service_parse_api_args(ngx_http_request_t *request, 
        ngx_str_t *base_url, const char ** account_id, int * account_id_size, 
        unsigned char ** video_id, ngx_str_t * ipAddress, int *width, int *height){

    static const ngx_str_t height_key = ngx_string("height");
    static const ngx_str_t width_key = ngx_string("width");
   
    // get publisher id
    unsigned char * publisher_id = neon_service_get_uri_token(request, base_url, 0);

    // get video id
    *video_id = neon_service_get_uri_token(request, base_url, 1);
    
    // get height and width
    ngx_str_t value;
    *height = 0;
    *width = 0;
    
    ngx_http_arg(request, height_key.data, height_key.len, &value);
    *height = neon_service_parse_number(&value);
    ngx_http_arg(request, width_key.data, width_key.len, &value);
    *width = neon_service_parse_number(&value);
   
    ngx_str_t cip_key = ngx_string("cip");
    ngx_http_arg(request, cip_key.data, cip_key.len, ipAddress);
    
    //static ngx_str_t xf = ngx_string("X-Client-IP");
    static ngx_str_t xf = ngx_string("X-Forwarded-For");
    ngx_table_elt_t * xf_header;
    
    //Check if CIP argument is present, else look for the header
    // TODO: Validate the IPAddress string
    if(ipAddress->len == 0 || ipAddress->len > 15){
        xf_header = search_headers_in(request, xf.data, xf.len); 
        if (xf_header){
            *ipAddress = xf_header->value;
        }
    }
    
    NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_ERROR error_account_id =
        neon_mastermind_account_id_lookup((char*)publisher_id,
                                          account_id,
                                          account_id_size);
        
    if(error_account_id != NEON_MASTERMIND_ACCOUNT_ID_LOOKUP_OK) {
        neon_stats[NEON_SERVER_API_ACCOUNT_ID_NOT_FOUND] ++;    
        return 1;
    }
    return 0;
} 

static void
neon_service_server_api_not_found(ngx_http_request_t *request,
                                         ngx_chain_t  * chain)
{
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

static void
neon_service_server_api_img_url_found(ngx_http_request_t *request,
        ngx_chain_t  * chain,
        char * url,
        int url_len)
{
    static ngx_str_t response_body_start = ngx_string("{\"data\":\"");
    static ngx_str_t response_body_end = ngx_string("\",\"error\":\"\"}");
       
    u_char * response_body = 0, *p = 0;
    int response_body_len = 0;
    response_body_len = response_body_start.len + response_body_end.len + url_len;
    response_body = ngx_pnalloc(request->pool, response_body_len);
    if (response_body == NULL){
        //TODO: send a 500 error ? 
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
                                ngx_chain_t  * chain)
{
    ngx_buf_t * b;
    b = ngx_pcalloc(request->pool, sizeof(ngx_buf_t));
    if(b == NULL){
        neon_stats[NGINX_OUT_OF_MEMORY] ++;
        return NEON_SERVER_API_FAIL;
    } 
    
    chain->buf = b;
    chain->next = NULL;
    
    ngx_str_t base_url = ngx_string("/v1/server/");
   
    const char * account_id = 0;
    unsigned char * video_id = 0;
    int account_id_size;
    ngx_str_t ipAddress = ngx_string("");
    int width;
    int height;

    int ret = neon_service_parse_api_args(request, &base_url, &account_id, 
                       &account_id_size, &video_id, &ipAddress, &width, &height);

    if(ret !=0 ){
        neon_stats[NEON_SERVER_API_ACCOUNT_ID_NOT_FOUND] ++;    
        neon_service_server_api_not_found(request, chain);
        return NEON_SERVER_API_FAIL;
    }
    
    // look up thumbnail image url
    
    const char * url = 0;
    int url_size = 0;
    
    NEON_MASTERMIND_IMAGE_URL_LOOKUP_ERROR error_url =
        neon_mastermind_image_url_lookup(account_id,
                (char*)video_id,
                &ipAddress,
                height,
                width,
                &url,
                &url_size);
    
    if(error_url != NEON_MASTERMIND_IMAGE_URL_LOOKUP_OK) {
        neon_stats[NEON_SERVER_API_URL_NOT_FOUND] ++;
        neon_service_server_api_not_found(request, chain);
        return NEON_SERVER_API_FAIL;
    }

    neon_service_server_api_img_url_found(request, chain, (char *)url, url_size); 
    return NEON_SERVER_API_OK;
}

static void
neon_service_client_api_not_found(ngx_http_request_t *request,
                                     ngx_chain_t  * chain)
{
    static ngx_str_t error_response_body = ngx_string("image not found");
    
    ngx_buf_t * b;
    b = (ngx_buf_t *) ngx_pcalloc(request->pool, sizeof(ngx_buf_t));
    if(b == NULL){
        neon_stats[NGINX_OUT_OF_MEMORY] ++;
        return;
    } 
    
    chain->buf = b;
    chain->next = NULL;
    
    request->headers_out.status = NGX_HTTP_NO_CONTENT;  // 204
    request->headers_out.content_type.len = sizeof("text/plain") - 1;
    request->headers_out.content_type.data = (u_char *) "text/plain";
    request->headers_out.content_length_n = error_response_body.len;
    b->pos = error_response_body.data;
    b->last = error_response_body.data + error_response_body.len;
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
                                    int url_size)
{
    // Do we need to send this data in the redirect ? 
    static ngx_str_t redirect_response_body = ngx_string("redirect to image");
    static ngx_str_t location_header = ngx_string("Location");

    ngx_buf_t * b;
    b = (ngx_buf_t *) ngx_pcalloc(request->pool, sizeof(ngx_buf_t));
    if(b == NULL){
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
 * input: video_id, height, width, IP address
 *
 * @return: NEON_CLIENT_API_OK or NEON_CLIENT_API_FAIL 
 */

NEON_CLIENT_API_ERROR
neon_service_client_api(ngx_http_request_t *request,
                                ngx_chain_t  * chain)
{
    ngx_buf_t * b;
    
    b = ngx_pcalloc(request->pool, sizeof(ngx_buf_t));
    if(b == NULL){
        neon_stats[NGINX_OUT_OF_MEMORY] ++;
        return NEON_CLIENT_API_FAIL;
    } 
    
    chain->buf = b;
    chain->next = NULL;
    
    ngx_str_t base_url = ngx_string("/v1/client/");
   
    const char * account_id = 0;
    unsigned char * video_id = 0;
    int account_id_size;
    ngx_str_t ipAddress = ngx_string("");
    int width;
    int height;

    int ret = neon_service_parse_api_args(request, &base_url, &account_id, 
                       &account_id_size, &video_id, &ipAddress, &width, &height);
        
    if (ret !=0){ 
        neon_stats[NEON_CLIENT_API_ACCOUNT_ID_NOT_FOUND] ++;
        neon_service_client_api_not_found(request, chain);
        return NEON_CLIENT_API_FAIL;
    }
    
    // look up thumbnail image url
    
    const char * url= 0;
    int url_size = 0;

    NEON_MASTERMIND_IMAGE_URL_LOOKUP_ERROR error_url =
        neon_mastermind_image_url_lookup(account_id,
                (char*)video_id,
                &ipAddress,
                height,
                width,
                &url,
                &url_size);

    if(error_url != NEON_MASTERMIND_IMAGE_URL_LOOKUP_OK) {
        neon_stats[NEON_CLIENT_API_URL_NOT_FOUND] ++;
        neon_service_client_api_not_found(request, chain);
        return NEON_CLIENT_API_FAIL;
    }

    // Check if the cookie is present
    if (neon_service_isset_neon_cookie(request) == NEON_FALSE){
        if(neon_service_set_neon_cookie(request) == NEON_TRUE) {
            neon_log_error("Neon cookie has been set");
        }    
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
                            ngx_chain_t  * chain)
{

    int clen = 0; 

    ngx_str_t base_url = ngx_string("/v1/getthumbnailid/");
    ngx_str_t params_key = ngx_string("params");
    ngx_str_t video_ids; 
    ngx_http_arg(request, params_key.data, params_key.len, &video_ids);

    // Response 
    // {
    //   "vid1": "Thumbnail ID 1",
    //     "vid2": "Thumbnail ID 2"
    // }
    //
    
    ngx_str_t ipAddress;
    static ngx_str_t xf = ngx_string("X-Forwarded-For");
    ngx_table_elt_t * xf_header;
    xf_header = search_headers_in(request, xf.data, xf.len); 

    // Check if X-Forwarded-For header is set (Its set by the load balancer)
    if (xf_header){
        ipAddress = xf_header->value;
    }

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
        neon_service_client_api_not_found(request, chain);
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
            
        NEON_MASTERMIND_TID_LOOKUP_ERROR err =
            neon_mastermind_tid_lookup(account_id,
                    (const char*)video_id,
                    &ipAddress,
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
