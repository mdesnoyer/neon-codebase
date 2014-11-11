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
                                unsigned char * s3downloader_fpath, 
                                unsigned char * s3_port, 
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
    
    neon_log_error("neon updater started");
  
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
        
            neon_log_error("mastermind expired");
            /*
             *  fetch new mastermind file from S3
             */
            if(neon_fetch(mastermind_url, mastermind_filepath, s3downloader, s3port, fetch_timeout) == NEON_FETCH_FAIL) {
                
                // log
                neon_log_error("failed to fetch mastermind file, error: %s",
                               neon_fetch_error);
                // alert
                neon_stats[NEON_UPDATER_HTTP_FETCH_FAIL]++; 
                neon_sleep(sleep_time);
                continue;
            }
            
            neon_stats[MASTERMIND_FILE_FETCH_SUCCESS]++; 
            
            /*
             *  validate meta data of new file 
             */
            
           	// TODO(Sunil) : Spawn a process to validate the Expiry
            time_t new_mastermind_expiry = neon_get_expiry(mastermind_filepath);
           	if (new_mastermind_expiry < time(0)){
                
                // Check if the expiry is greater than the current expiry, if
                // Yes Load IT !
                
                if(neon_mastermind_is_expiry_greater_than_current(new_mastermind_expiry) == NEON_TRUE){
                    neon_log_error("mastermind file has expired, But loading new one since expiry is greater");
                    neon_stats[NEON_UPDATER_MASTERMIND_EXPIRED]++;
                }else{ 
                    neon_log_error("mastermind file has expired and older than current file being used, hence not loading it");
                    neon_stats[NEON_UPDATER_MASTERMIND_EXPIRED]++;
                    neon_sleep(sleep_time);
                    continue;
                }
			} 
            
            /*
             *  parse and process new mastermind file into memory
             */
            // process file into memory
            if(neon_mastermind_load(mastermind_filepath) == NEON_LOAD_FAIL) {
                
                // the load function will log the specific error
                neon_log_error("failed to load mastermind file");
                
                // alert
                neon_stats[NEON_UPDATER_MASTERMIND_LOAD_FAIL]++;
                neon_sleep(sleep_time);
                continue;
            }
            
            /*
             *  rename mastermind file as validated
             */
            if( neon_rename(mastermind_filepath, validated_mastermind_filepath) == NEON_RENAME_FAIL) {
        
                // log
                neon_log_error("failed to rename mastermind file, error: %s",
                               neon_rename_error);
                
                // alert
                neon_stats[NEON_UPDATER_MASTERMIND_RENAME_FAIL]++;
                neon_sleep(sleep_time);
                continue;
            }

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




