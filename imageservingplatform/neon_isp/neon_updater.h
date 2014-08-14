#ifndef _NEON_UPDATER_
#define _NEON_UPDATER_

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "neon_error_codes.h"

/*
 * Config Init method
 * */

void neon_updater_config_init(unsigned char *, unsigned char *, unsigned char *, unsigned char *, time_t);

/*
 *  Starts updater thread
 */
int neon_start_updater();


/*
 *  Terminate updater thread
 */
int neon_terminate_updater();


/*
 *  Main runloop of updater thread
 */
void * neon_runloop(void * arg);

/*
typedef struct {
	ngx_str_t mastermind_file_url;
} ngx_http_neon_loc_conf_t;

extern ngx_str_t mastermind_file_url_str;
*/

#endif

