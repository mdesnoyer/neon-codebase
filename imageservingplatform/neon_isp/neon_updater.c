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
#include "neon_publisher_hashtable.h"


// updater thread
static pthread_t neon_updater_thread;

// optional thread attributes
//static pthread_attr_t neon_attr;

// shutdown flag for updater thread
static int neon_shutdown = 0;


// Configuration
static time_t sleep_time = 10;
static time_t fetch_timeout = 30;
static const char * mastermind_url = 0; //"http://neon-test.s3.amazonaws.com/mastermind";
static const char * mastermind_filepath             = "/tmp/mastermind";
static const char * validated_mastermind_filepath   = 0; //"/tmp/mastermind.validated";

static const int test_config = 1;

/*
 * Initialize the config parameters for the updater thread
 *
 * */
void neon_updater_config_init(unsigned char *m_url, unsigned char *m_valid_path, time_t s_time)
{
    // Mastermind REST URI
	if (mastermind_url == 0)
		mastermind_url = strdup((const char *)m_url);
	else
		free((void *) mastermind_url);

    // Validater Mastermind file path
    if (m_valid_path == 0)
        validated_mastermind_filepath = strdup((const char *)m_valid_path);
    else
        free((void*) validated_mastermind_filepath);

    // Updater Sleep time in seconds
    sleep_time = s_time;
}	


void *
neon_runloop(void * arg)
{
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
    
    neon_log_error("Neon updater run loop");
    while(neon_shutdown != NEON_TRUE)
    {
        // check if the current mastermind's expiry is approaching
        if(neon_mastermind_expired() == NEON_TRUE)
        {
        
            neon_log_error("mastermind expired");
            /*
             *  fetch new mastermind file from S3
             */
            if( neon_fetch(mastermind_url, mastermind_filepath, fetch_timeout) == NEON_FETCH_FAIL) {
                
                // log
                neon_log_error("failed to fetch mastermind file, error: %s",
                               neon_fetch_error);
                // alert
                
                neon_sleep(sleep_time);
                continue;
            }
            
            /*
             *  validate meta data of new file 
             */
            
           	// TODO(Sunil) : Spawn a process to validate the Expiry 
           	if (neon_get_expiry(mastermind_filepath) < time(0)){
                neon_log_error("mastermind file has expired, hence not loading it");

                neon_sleep(sleep_time);
				continue;	
			} 
            
            /*
             *  parse and process new mastermind file into memory
             */
            // process file into memory
            if( neon_mastermind_load(mastermind_filepath) == NEON_LOAD_FAIL) {
                
                // the load function will log the specific error
                neon_log_error("failed to load mastermind file");
                
                // alert
                
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
                
                neon_sleep(sleep_time);
                continue;
            }
        }
        
        // go to sleep until next cycle
        
    }
    
    
    return 0;
}


int
neon_start_updater()
{
    
    int ret = 0;
    long t = 0;
    
    //neon_log(NEON_DEBUG, "starting updater thread");
    //system("echo starting_updater >> /tmp/neon_echo_log");
    
    ret = pthread_create(&neon_updater_thread, NULL, neon_runloop, (void *)t);
    
    if(ret != 0) {
        
    }
    
    return 0;
}


int
neon_terminate_updater()
{
    neon_shutdown = 1;
    
    int ret = pthread_join(neon_updater_thread, 0);
    
    if(ret != 0)
        neon_log_error("cannot stop updater thread");
    
    return 0;
}






