/*
 * Download the mastermind file from S3 via a custom s3downloader script
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include "neon_log.h"
#include "neon_stats.h"
#include "neon_fetch.h"

/*****************************************************************************
Name : neon_fetch 
Parameters : mastermind_url - url where mastermind is located 
             mastermind_filepath - exact filepath to mastermind 
             s3port - port of s3 
             s3downloader_path - where the s3 downloader is located should 
                                 be a .py file 
             timeout - when to timeout the call 
             script_output - allocated memory for script_output caller is responsible 
                         for freeing this 
Return Value : output of the system call
Notes : allocate memory for script_output --- caller must free 
******************************************************************************/

NEON_FETCH_ERROR
neon_fetch(const char * const mastermind_url,
           const char * const mastermind_filepath,
           const char * const s3port,
           const char * const s3downloader_path,
           time_t timeout, 
           char ** script_output)
{
    FILE *script_runner;
    char *line = NULL; 
    size_t len = 0, current_size=0, output_size=1024;
    ssize_t read;   
    static char command[1024];
    int ret = 0;

    *script_output = (char *)malloc(output_size);
    strcat(*script_output, "neon_fetch::isp_s3downloader: ");
 
    static const char * format = "/usr/local/bin/isp_s3downloader -u %s -d %s";
    if (s3port != NULL && s3downloader_path != NULL) {
        format = "%s -u %s -d %s -s localhost -p %s";
        if (sprintf(command, format, s3downloader_path, mastermind_url, mastermind_filepath, s3port) <= 0){
            strcat(*script_output, "ERROR : failed sprintfing the command");  
            return NEON_FETCH_FAIL;
        }
    }
    else {
        sprintf(command, format, mastermind_url, mastermind_filepath); 
    }
    
    errno = 0;
    script_runner = popen(command, "r"); 
 
    if (script_runner == NULL) {
        strcat(*script_output, "ERROR : failed to open up the script with popen");  
        return NEON_FETCH_FAIL;
    }

    while ( (read = getline(&line, &len, script_runner)) != -1) { 
         current_size += read;     
         if (current_size >= output_size) { 
             char *temp = realloc(*script_output, output_size*2); 
             if (!temp) { 
                 strcat(*script_output, "ERROR : realloc error in fetching");  
                 return NEON_FETCH_FAIL; 
             }
             output_size *= 2; 
             *script_output = temp; 
         }
         strcat(*script_output, line);  
    }
 
    free(line); 
    ret = WEXITSTATUS(fclose(script_runner)); 
    
    return ret == 0 ? NEON_FETCH_OK : NEON_FETCH_FAIL;
}


