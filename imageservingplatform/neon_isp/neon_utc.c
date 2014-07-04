#define _XOPEN_SOURCE
#include <time.h>
#include <string.h>
#include "neon_utc.h"


const char * neon_utc_error = 0;


NEON_UTC_ERROR
neon_convert_string_to_time(const char * const str, time_t * result)
{
    // ISO-8601  "2014-03-27T23:20:00Z"
    
    struct tm when;
    
    // this function takes care of proper conversion to a struct tm, with proper
    // year and month numbers
    strptime(str, "%Y-%m-%dT%H:%M:%SZ", &when);
    
    // validate
    
    // in ISO 8601 the month number go from 1 to 12,  however for struct_tm the month
    // number is in the range of 0 to 11,  January being 0.  So a conversion is required
    // here
    //when.tm_mon -= 1;
    
    *result = mktime(&when);
    
    return NEON_UTC_OK;
}