#include <time.h>
#include <stdio.h>
#include <assert.h>
#include "neon_utc.h"
#include "neon_error_codes.h"

int
main(int argc, char ** argv)
{
    
    // ISO-8601  "2014-03-27T23:20:00Z"
    
    {
        const char * str = "2014-01-02T01:02:03Z";
        time_t correct = 1388624523;
        
        time_t result = 0;
        int ret = neon_convert_string_to_time(str, &result);
        
        assert( (ret == NEON_UTC_OK) && (correct == result) );
    }


}

