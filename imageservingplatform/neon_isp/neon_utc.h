#ifndef _NEON_UTC_
#define _NEON_UTC_


#include "neon_error_codes.h"


typedef enum  {
    
    NEON_UTC_OK = 0,
    NEON_UTC_PARSE_ERROR
    
} NEON_UTC_ERROR;



extern const char * neon_utc_error;


/*
 *  Converts a ISO-8601 timestamp into a time_t 
 */
NEON_UTC_ERROR neon_convert_string_to_time(const char * const str, time_t * t);



#endif


