/*
 * Neon utilities 
 *
 * */

#include <time.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

#include "neon_error_codes.h"
#include "neon_stats.h"
#include "neon_utc.h"
#include "neon_utils.h"



NEON_BOOLEAN
neon_check_file_exist(const char * const filepath)
{
    errno = 0;
    FILE * f = fopen(filepath, "r");
    
    if(f != NULL)
        return NEON_TRUE;
    
    return NEON_FALSE;
}


time_t
neon_get_expiry(const char * const filepath)
{
    errno = 0;
    FILE * file = fopen(filepath, "r");
    
    time_t gmt = neon_get_expiry_from_file(file);
    
    errno = 0;
    fclose(file);
    
    return gmt;
}


time_t
neon_get_expiry_from_file(FILE * file)
{
    char buffer[32];
    char * line = 0;
    long position = 0;
    
    // note where is the file position currently
    position = ftell(file);
    
    // position first byte to read expiry
    errno = 0;
    fseek(file, 0L, SEEK_SET);
    
    // get the expiry line
    line = fgets(buffer, 32, file);
    
    // set file position to where it was
    errno = 0;
    fseek(file, position, SEEK_SET);
    
    // must begin
    if(strncmp(line, "expiry=", 7) != 0)
        return 0;
    
    // jump to date string
    line += 7;
    
    time_t gmt = 0;
    neon_convert_string_to_time(line, &gmt);
    
    return gmt;
}


NEON_BOOLEAN
neon_check_expired(time_t expiry)
{
    time_t now = time(0);

    if(now >= expiry)
        return NEON_TRUE;
    
    return NEON_FALSE;
}


NEON_BOOLEAN
neon_sleep(time_t seconds)
{
    struct timespec d;
    d.tv_sec = seconds;
    d.tv_nsec = 0;
    nanosleep(&d, 0);
    
    return NEON_TRUE;
}


NEON_RENAME_ERROR
neon_rename(const char * const old_filepath, const char * const new_filepath)
{
    int ret = 0;
    
    errno = 0;
    ret = rename(old_filepath, new_filepath);
    
    if(ret != 0) {
        neon_rename_error = strerror(errno);
        return NEON_RENAME_FAIL;
    }
    
    return NEON_RENAME_OK;
}



// String hash function 

unsigned long
neon_sdbm_hash(unsigned char *str, int s_len) 
{
    unsigned long hash = 0;
    int c, i=0;

    if(str){
        while ((c = *str++) && i++ < s_len){
            hash = c + (hash << 6) + (hash << 16) - hash;
        }
    }

    return hash;
}


// Get Random string

void neon_get_uuid(char *dest, size_t length){
	static const char charset[] =
		"0123456789"
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		"abcdefghijklmnopqrstuvwxyz";

    //  62 characters, last one being at index 61
    static const int charset_max_index = 10 + 26 + 26 - 1;

    while (length-- > 0) {
        size_t index = (double) rand() / RAND_MAX * charset_max_index;
        *dest++ = charset[index];
    }
  
    *dest = '\0';
}

// Verify if the given string is a valid ip address

NEON_BOOLEAN neon_is_valid_ip_string(unsigned char *ip){
  struct sockaddr_in sa;
  int result = inet_pton(AF_INET, (char *)ip, &(sa.sin_addr)); // only IPV4
  if (result <= 0) 
    return NEON_FALSE; 
  return NEON_TRUE;
}

const char * neon_rename_error = 0;
