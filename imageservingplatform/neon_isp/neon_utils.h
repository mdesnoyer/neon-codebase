#ifndef _NEON_UTILS_
#define _NEON_UTILS_

#include <stdio.h>
#include <time.h>
#include "neon_error_codes.h"


/*
 *
 */
NEON_BOOLEAN neon_check_file_exist(const char * const filepath);


/*
 *
 */
time_t neon_get_expiry(const char * const filepath);


/*
 *
 */
time_t neon_get_expiry_from_file(FILE * file);


/*
 *
 */
NEON_BOOLEAN neon_check_expired(time_t expiry);


/*
 *
 */
NEON_BOOLEAN neon_sleep(time_t seconds);


/*
 *
 */
typedef enum  {
    
    NEON_RENAME_OK = 0,
    NEON_RENAME_FAIL
    
} NEON_RENAME_ERROR;

NEON_RENAME_ERROR neon_rename(const char * const oldname, const char * const newname);

extern const char * neon_rename_error;

unsigned long neon_sdbm_hash(unsigned char *str, int s_len);


/*
 * Fills a string with n number of random alphanuneric characters. A terminating zero is
 * added after, so the string must be at least of n+1 size. Behavior is undefined if
 * a null pointer or a shorter string is supplied.
 */
void neon_get_uuid(char *dest, size_t n);



NEON_BOOLEAN neon_is_valid_ip_string(unsigned char *ip);

#endif


