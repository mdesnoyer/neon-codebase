/*
 * The Image Serving system GOD Module. This is the entry point in to the
 * module as far as NGINX is concerned.
 *
 * For nginx data types refer -
 * http://antoine.bonavita.free.fr/nginx_mod_dev_en.html
 *
*/

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include "neon_cfg.h"
#include "neon_log.h"
#include "neon_mastermind.h"
#include "neon_service.h"
#include "neon_stats.h"
#include "neon_updater.h"


/*
 *  These function install request handlers
 */
static char *ngx_http_neon_client_hook(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_neon_server_hook(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_neon_getthumbnailid_hook(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_neon_healthcheck_hook(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_neon_stats_hook(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_neon_mastermind_file_url(ngx_conf_t *cf, void *post, void *data);
static char *ngx_http_neon_mastermind_validated_filepath(ngx_conf_t *cf, void *post, void *data);

static ngx_int_t ngx_http_neon_handler_healthcheck(ngx_http_request_t *r);
static ngx_conf_post_handler_pt ngx_http_neon_mastermind_file_url_p = ngx_http_neon_mastermind_file_url;
static ngx_conf_post_handler_pt ngx_http_neon_mastermind_validated_filepath_p = ngx_http_neon_mastermind_validated_filepath;

/*
 * Neon specific configuration structure
 *
 * */
typedef struct {
    ngx_str_t mastermind_file_url;
    time_t updater_sleep_time;
    time_t updater_fetch_timeout;
    ngx_str_t mastermind_filepath;
    ngx_str_t mastermind_validated_filepath;
} ngx_http_neon_loc_conf_t;

static ngx_str_t mastermind_file_url_str;
static ngx_str_t mastermind_validated_filepath;

/*
 *  Our directives in config
 *  The APIs available to the outside world
 */
static ngx_command_t  ngx_http_neon_commands[] = {

    { ngx_string("v1_client"),
      NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
      ngx_http_neon_client_hook,
      0,
      0,
      NULL },
  
    { ngx_string("v1_server"),
        NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
        ngx_http_neon_server_hook,
        0,
        0,
        NULL },
    
    { ngx_string("v1_getthumbnailid"),
        NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
        ngx_http_neon_getthumbnailid_hook,
        0,
        0,
        NULL },
    
    { ngx_string("mastermind_stats"),
        NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
        ngx_http_neon_stats_hook,
        0,
        0,
        NULL },
    
    { ngx_string("mastermind_healthcheck"),
        NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
        ngx_http_neon_healthcheck_hook,
        0,
        0,
        NULL },
    
    { ngx_string("mastermind_file_url"),
        NGX_HTTP_MAIN_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_neon_loc_conf_t, mastermind_file_url),
        &ngx_http_neon_mastermind_file_url_p },
    
    { ngx_string("mastermind_validated_filepath"),
        NGX_HTTP_MAIN_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_neon_loc_conf_t, mastermind_validated_filepath),
        &ngx_http_neon_mastermind_validated_filepath_p },

    ngx_null_command
};


/* Create loc conf
 * It takes a directive struct (ngx_conf_t) and returns a newly created module configuration struct
 * 
 */
static void *
ngx_http_neon_create_loc_conf(ngx_conf_t * cf)
{
    ngx_http_neon_loc_conf_t * conf;
    
    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_neon_loc_conf_t));
    
    if(conf == NULL) {
        return NGX_CONF_ERROR;
    }
    
    //conf->mastermind_url = NGX_CONF_UNSET;
    return conf;
}


static char *
ngx_http_neon_merge_loc_conf(ngx_conf_t * cf, void * parent, void * child)
{
    
    //ngx_http_neon_loc_conf_t * prev = parent;
    //ngx_http_neon_loc_conf_t * conf = child;
    
    
    return NGX_CONF_OK;
}

/*
 * Get the location 
 * */
static char *
ngx_http_neon_mastermind_file_url(ngx_conf_t *cf, void *post, void *data)
{
    //ngx_http_core_loc_conf_t *clcf;

    //clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    //clcf->handler = ngx_http_neon_handler_healthcheck; // which handler should this be ? 

    ngx_str_t  *name = data; // i.e., first field of var
   
       if(name == NULL){
        return NGX_CONF_ERROR;
    }    
    
    if (ngx_strcmp(name->data, "") == 0) {
        return NGX_CONF_ERROR;
    }

    mastermind_file_url_str.data = name->data;
    mastermind_file_url_str.len = ngx_strlen(mastermind_file_url_str.data);

    return NGX_CONF_OK;
}

/*
 * Mastermind file path
 * */
static char *
ngx_http_neon_mastermind_validated_filepath(ngx_conf_t *cf, void *post, void *data)
{
    ngx_str_t  *name = data; // i.e., first field of var
   
       if(name == NULL){
        return NGX_CONF_ERROR;
    }    
    
    if (ngx_strcmp(name->data, "") == 0) {
        return NGX_CONF_ERROR;
    }
    
    mastermind_validated_filepath.data = name->data;
    mastermind_validated_filepath.len = ngx_strlen(name->data);
    
    neon_log_error("Parsed mastermind fp %s", mastermind_validated_filepath.data);
    return NGX_CONF_OK;
}

/* Module Context
 *
 * This is a static ngx_http_module_t struct, which just has a bunch of function
 * references for creating the three configurations and merging them together. 
 * Its name is ngx_http_<module name>_module_ctx. In order, the function references are:
 *
 * preconfiguration
 * postconfiguration
 * creating the main conf (i.e., do a malloc and set defaults)
 * initializing the main conf (i.e., override the defaults with what's in nginx.conf)
 * creating the server conf
 * merging it with the main conf
 * creating the location conf
 * merging it with the server conf
 */
static ngx_http_module_t  ngx_http_neon_module_ctx = {
    NULL,                          /* preconfiguration */
    NULL,                          /* postconfiguration */

    NULL,                          /* create main configuration */
    NULL,                          /* init main configuration */

    ngx_http_neon_create_loc_conf, /* create server configuration */
    ngx_http_neon_merge_loc_conf,  /* merge server configuration */
    ngx_http_neon_create_loc_conf, /* create location configuration */
    ngx_http_neon_merge_loc_conf   /* merge location configuration */
};

// Module Hook Methods

ngx_int_t neon_init_process(ngx_cycle_t *cycle);
void neon_exit_process(ngx_cycle_t *cycle);
ngx_int_t neon_init_module(ngx_cycle_t *cycle);


/* Module Definition
 * This is one more layer of indirection, the ngx_module_t struct. 
 * The variable is called ngx_http_<module name>_module. 
 * This is where references to the context and directives go, as well as the 
 * remaining callbacks (exit thread, exit process, etc.). 
 * The module definition is sometimes used as a key to look up data associated with a particular module
 *
 */
ngx_module_t ngx_http_neon_module = {
  NGX_MODULE_V1,
  &ngx_http_neon_module_ctx,  /* module context */
  ngx_http_neon_commands,     /* module directives */
  NGX_HTTP_MODULE,            /* module type */
  NULL,                       /* init master */
  neon_init_module,           /* init module */
  &neon_init_process,         /* init process */
  NULL,                       /* init thread */
  NULL,                       /* exit thread */
  &neon_exit_process,         /* exit process */
  NULL,                       /* exit master */
  NGX_MODULE_V1_PADDING
};



ngx_int_t neon_init_process(ngx_cycle_t *cycle)
{
       neon_updater_config_init(mastermind_file_url_str.data); 
    neon_start_updater();
    return 0;
}


void
neon_exit_process(ngx_cycle_t *cycle)
{
    neon_terminate_updater();
}




ngx_int_t
neon_init_module(ngx_cycle_t *cycle)
{
    return 0;
}

////////////////// Image Server API Handlers ////////////////////////


/*
 *    Server image serving handler
 *
 */

static ngx_int_t ngx_http_neon_handler_server(ngx_http_request_t *request)
{
    neon_log_error("INFO: Server API Handler");
    
    ngx_chain_t chain;
    
    neon_service_server_api(request, &chain);
    
    ngx_http_send_header(request);
    
    return ngx_http_output_filter(request, &chain);
}



/*
 *    Client image serving handler
 *
 */

// uri 20 /v1/client/toto/tata?v

static ngx_int_t ngx_http_neon_handler_client(ngx_http_request_t *request)
{
    ngx_chain_t  chain;
 
    neon_service_client_api(request, &chain);
    
    ngx_http_send_header(request);
    
    return ngx_http_output_filter(request, &chain);
}


/*
 *    GetThumbnail image serving handler
 *
 */

static ngx_int_t ngx_http_neon_handler_getthumbnailid(ngx_http_request_t *request)
{
    ngx_chain_t   chain;
    
    neon_service_getthumbnailid(request, &chain);

    ///////////
    //  config
    //////////
    /*
     ngx_http_neon_loc_conf_t  * neon_config = 0;
     neon_config = ngx_http_conf_get_module_loc_conf(r, ngx_http_neon_module);
     
     neon_log_error("ttatatatatat");
     
     if(neon_config != NULL) {
     if(neon_config->mastermind_url.len == 0)
     neon_log_error("neon_config->mastermind_url is zero");
     }
     else
     neon_log_error("neon_config is null");
     
     neon_log_error("titititititi");
     */
    
    ngx_http_send_header(request);
    
    return ngx_http_output_filter(request, &chain);
}


/*
 *    Health check handler
 *
 */

static ngx_int_t ngx_http_neon_handler_healthcheck(ngx_http_request_t *r)
{
    static char pageInServiceCurrent[] = "In service with current Mastermind";
    static char pageInServiceExpired[] = "In service with expired Mastermind";
    static char pageOutOfService[] = "Out of service";
    ngx_buf_t    *b;
    ngx_chain_t   out;
    
    r->headers_out.content_type.len = sizeof("text/plain") - 1;
    r->headers_out.content_type.data = (u_char *) "text/plain";
    
    b = ngx_pcalloc(r->pool, sizeof(ngx_buf_t));
    
    out.buf = b;
    out.next = NULL;
    
    char * body = 0;
    
    int status = neon_mastermind_healthcheck();
    
    // no mastermind data available, no service
    if(status == 0) {
        r->headers_out.status = NGX_HTTP_SERVICE_UNAVAILABLE ;
        body = pageOutOfService;
    }
    
    // mastermind data available
    else {
        
        r->headers_out.status = NGX_HTTP_OK;
       
           //TODO: status to be 400 for others
        
        // expired but otherwise serviceable
        if(status == 1)
            body = pageInServiceExpired;
        // status 2, mastermind data is current
        else
            body = pageInServiceCurrent;
    }
    
    b->pos = (unsigned char*)body;
    b->last = (unsigned char*)body + strlen((char*)body);
    
    b->memory = 1;
    b->last_buf = 1;
    
    r->headers_out.content_length_n = strlen((char*)body);
    ngx_http_send_header(r);
    
    return ngx_http_output_filter(r, &out);
}


/*
 *    Stats handler
 *
 */

static ngx_int_t ngx_http_neon_handler_stats(ngx_http_request_t *r)
{
    static u_char stats_response[] = 
    "{\"MASTERMIND_FILE_FETCH_SUCCESS\": %llu "
    ",\"MASTERMIND_FILE_FETCH_FAIL\" : %llu"
    ",\"MASTERMIND_PARSE_SUCCESS\" : %llu"
    ",\"MASTERMIND_PARSE_FAIL\" : %llu"
    ",\"MASTERMIND_RENAME_SUCCESS\" : %llu"
    ",\"MASTERMIND_RENAME_FAIL\" : %llu"
    ",\"NEON_SERVICE_TOKEN_FAIL\" : %llu"
    ",\"NEON_SERVICE_TOKEN_NOT_FOUND\" : %llu"
    ",\"NEON_SERVICE_COOKIE_PRESENT\" : %llu"
    ",\"NEON_SERVICE_COOKIE_SET\" : %llu"
    ",\"NEON_SERVICE_COOKIE_SET_FAIL\" : %llu"
    ",\"NEON_CLIENT_API_ACCOUNT_ID_NOT_FOUND\": %llu"
    ",\"NEON_CLIENT_API_URL_NOT_FOUND\": %llu"
    ",\"NEON_SERVER_API_ACCOUNT_ID_NOT_FOUND\": %llu"
    ",\"NEON_SERVER_API_URL_NOT_FOUND\": %llu"

    "}";

    static u_char resp[2048];

    //static unsigned char test_response[] = "stats";
    ngx_buf_t    *b;
    ngx_chain_t   out;
    
    r->headers_out.content_type.len = sizeof("text/plain") - 1;
    r->headers_out.content_type.data = (u_char *) "text/plain";
    
    
    neon_log_error("stats");
    
    b = ngx_pcalloc(r->pool, sizeof(ngx_buf_t));
    
    out.buf = b;
    out.next = NULL;
  
    // fill the stats response
    sprintf((char *) resp,
          (const char *) stats_response,
          neon_stats[0],
          neon_stats[1],
          neon_stats[2],
          neon_stats[3],
          neon_stats[4],
          neon_stats[5],
          neon_stats[6],
          neon_stats[7],
          neon_stats[8],
          neon_stats[9],
          neon_stats[10],
          neon_stats[11],
          neon_stats[12],
          neon_stats[13],
          neon_stats[14]
          );
    
    b->pos = resp;
    b->last = (u_char*)resp + strlen((char*)resp);
    
    b->memory = 1;
    b->last_buf = 1;
    
    r->headers_out.status = NGX_HTTP_OK;
    r->headers_out.content_length_n = strlen((char*)resp);
    
    ngx_http_send_header(r);
    
    return ngx_http_output_filter(r, &out);
}



/*
 *   Handlers installers
 */

static char *ngx_http_neon_client_hook(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
  ngx_http_core_loc_conf_t  *clcf;
  clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
  clcf->handler = ngx_http_neon_handler_client;

    
  return NGX_CONF_OK;
}


static char *ngx_http_neon_server_hook(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_core_loc_conf_t  *clcf;
    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_neon_handler_server;
    
    ngx_http_neon_mastermind_file_url(cf, cmd, conf);    
    return NGX_CONF_OK;
}


static char *ngx_http_neon_getthumbnailid_hook(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_core_loc_conf_t  *clcf;
    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_neon_handler_getthumbnailid;
    
    return NGX_CONF_OK;
}


static char *ngx_http_neon_healthcheck_hook(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_core_loc_conf_t  *clcf;
    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_neon_handler_healthcheck;
    
    return NGX_CONF_OK;
}


static char *ngx_http_neon_stats_hook(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_core_loc_conf_t  *clcf;
    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_neon_handler_stats;
    
    return NGX_CONF_OK;
}


