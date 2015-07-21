/*
 * Neon Mastermind data file Updater Thread
 *
 * Checks every 'x' seconds if a mastermind file has been published,
 * if available then downloads, validates and saves it on disk. 
 *
 * */

#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <signal.h>
#include <pthread.h>
#include "neon_updater.h"
#include "neon_mastermind.h" 
#include "neon_fetch.h"
#include "neon_utils.h"
#include "neon_stats.h"
#include "neon_log.h"


// updater thread
static pthread_t neon_updater_thread;

// optional thread attributes
//static pthread_attr_t neon_attr;

// shutdown flag for updater thread
static int neon_shutdown = 0;


// Configuration
static time_t sleep_time = 10;
static time_t fetch_timeout = 30;
static const char * mastermind_url = 0; // HTTP REST URL to marstermind file 
// TODO: Need to lock the download path if >1 worker process will be spun
static const char * mastermind_filepath             = 0; // download the file here
static const char * validated_mastermind_filepath   = 0; 
static const char * s3port = 0;
static const char * s3downloader = 0;
static const int test_config = 1;

/*
 * Initialize the config parameters for the updater thread
 *
 * */
void neon_updater_config_init(unsigned char *m_url, 
                                unsigned char *m_valid_path, 
                                unsigned char *m_download_path, 
                                unsigned char * s3_port, 
                                unsigned char * s3downloader_fpath, 
                                time_t s_time){
    
    // Mastermind REST URI
	if (mastermind_url == 0)
		mastermind_url = strdup((const char *) m_url);
	else
		free((void *) mastermind_url);

    // Validater Mastermind file path
    if (validated_mastermind_filepath == 0)
        validated_mastermind_filepath = strdup((const char *) m_valid_path);
    else
        free((void*) validated_mastermind_filepath);
    
    // Mastermind download file path
    if (mastermind_filepath == 0)
        mastermind_filepath = strdup((const char *) m_download_path);
    else
        free((void*) mastermind_filepath);

    // Updater Sleep time in seconds
    srand(time(NULL));
    int r = rand() % 100;
    sleep_time += s_time + r;
    //neon_log_error("NEON Config sleep time %d", sleep_time);
    
    // s3downloader filepath
	if (s3downloader == 0 && s3downloader_fpath != NULL)
		s3downloader = strdup((const char *) s3downloader_fpath);
	else
		free((void *) s3downloader);

    // s3cmd config filepath
	if (s3port == 0 && s3_port != NULL)
		s3port = strdup((const char *) s3_port);
	else
		free((void *) s3port);
}	

void *
neon_runloop(void * arg){
    
    // turn off all signals here
    /*
    sigset_t set;
    sigfillset(&set);
    sigprocmask(SIG_BLOCK, &set, NULL);
    */
  
   	// Enable the SIGCHLD default handler (else it doesnt work on ubuntu)
   	signal(SIGCHLD, SIG_DFL);	
    
    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "updater: started");
  
    neon_stats_init();
    
    neon_mastermind_init();
    
    // check if we have a validated mastermind data file exist on disk
	/*
	if(neon_check_file_exist(validated_mastermind_filepath) == NEON_TRUE)
	{

	// get the expiry date of the file
	time_t expiry = neon_get_expiry(validated_mastermind_filepath);

	// if not expired then load it in
	if(neon_check_expired(expiry) == NEON_FALSE)
	neon_mastermind_load(validated_mastermind_filepath);

	}
	*/
   
    ///////////////////////////////////////////
    //  Runloop
    ///////////////////////////////////////////
    
    while(neon_shutdown != NEON_TRUE){
        neon_log_error("Neon updater run loop");
        
        // check if the current mastermind's expiry is approaching
        if(neon_mastermind_expired() == NEON_TRUE){
        
            /*
             *  fetch new mastermind file from S3
             */
            char *error_msg = NULL; 
            if(neon_fetch(mastermind_url, mastermind_filepath, s3port, s3downloader, fetch_timeout, &error_msg) == NEON_FETCH_FAIL) {
                // log
                ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, 
                        "updater: failed to fetch mastermind file: %s", error_msg);
                neon_stats[NEON_UPDATER_HTTP_FETCH_FAIL]++; 
                neon_sleep(sleep_time);
                continue;
            }
            if (error_msg) {
                free(error_msg); 
            } 

            
            neon_stats[MASTERMIND_FILE_FETCH_SUCCESS]++; 
            
            /*
             *  Validate expiry of new file 
             */
            time_t new_mastermind_expiry = neon_get_expiry(mastermind_filepath);
            if (new_mastermind_expiry < time(0)){
                
                if(neon_mastermind_is_expiry_greater_than_current(new_mastermind_expiry) == NEON_TRUE){
                    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, 
                            "updater: fetched mastermind is expired but ahead of current");
                    neon_stats[NEON_UPDATER_MASTERMIND_EXPIRED]++;
                }else{ 
                    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, 
                            "updater: fetched mastermind is expired and older than current");
                    neon_stats[NEON_UPDATER_MASTERMIND_EXPIRED]++;
                    neon_sleep(sleep_time);
                    continue;
                }
	        } 
            
            /*
             *  Parse and process new mastermind file into memory
             */
            NEON_LOAD_ERROR load_error = neon_mastermind_load(mastermind_filepath);

            // success
            if(load_error == NEON_LOAD_OK || load_error == NEON_LOAD_PARTIAL) {

                // if some non-fatal errors were detected
                if(load_error == NEON_LOAD_PARTIAL) {
                    ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, "updater: some mastermind entries were rejected needing "
                        "prompt investigation: %s", neon_mastermind_error);            
                }
            }   
            // failure
            else {
                ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, 
                        "updater: mastermind load failed: error: %s", neon_mastermind_error);
                neon_stats[NEON_UPDATER_MASTERMIND_LOAD_FAIL]++;
                neon_sleep(sleep_time);
                continue; 
            }

            /*
             *  Rename mastermind file as validated
             */
            if( neon_rename(mastermind_filepath, validated_mastermind_filepath) == NEON_RENAME_FAIL) {
        
                ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, 
                        "updater: failed to rename validated mastermind file, error: %s", neon_rename_error);
                neon_stats[NEON_UPDATER_MASTERMIND_RENAME_FAIL]++;
                neon_sleep(sleep_time);
                continue;
            }

	        // log success
            ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0, 
                    "updater: fetched and loaded mastermind file successfully");
            neon_stats[MASTERMIND_RENAME_SUCCESS]++;
        }
        
        // go to sleep until next cycle
        neon_sleep(sleep_time);
        
    }

    neon_mastermind_shutdown();

    return 0;
}


/*
 * Updater thread creation 
 *
 * */

int
neon_start_updater(){
    
    int ret = 0;
    long t = 0;
    
    ret = pthread_create(&neon_updater_thread, NULL, neon_runloop, (void *)t);
    
    if(ret != 0) {
        
    }
    
    return 0;
}

/*
 * Updater thread shutdown
 * */

int
neon_terminate_updater(){
    
    neon_shutdown = 1;
    
    int ret = pthread_join(neon_updater_thread, 0);
    
    if(ret != 0)
        neon_log_error("cannot stop updater thread");

    return 0;
}




