#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>



ngx_table_elt_t *
search_headers_in(ngx_http_request_t *r, u_char *name, size_t len);



long 
neon_service_parse_number(ngx_str_t * value);


/*
 *  Removes a trailing ".jpg" from a string. Does nothing if none is 
 *  present. If present a terminating character is written over the "."
 *  to terminate the string, in effect removing the jpg extention.
 *  This function handles lowercase and uppercase occurences of 
 *  a jpg extention
 */
void
remove_jpg_extention(unsigned char * item);


