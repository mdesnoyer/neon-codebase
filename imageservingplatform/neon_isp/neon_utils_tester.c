#include <time.h>
#include <stdio.h>
#include <assert.h>
#include "neon_utc.h"
#include "neon_utils.h"
#include "neon_error_codes.h"

int
main(int argc, char ** argv)
{
    
     
    {
        // date in mastermind.test "2014-01-02T01:02:03Z";
        time_t correct = 1388624523;
        
        time_t result = neon_get_expiry("./mastermind.test");
        
        assert(correct == result);
    }
    
    {
        int ret = neon_check_file_exist("./mastermind.validated.test");
        assert(ret == NEON_TRUE);
        
        ret = neon_check_file_exist("./mastermind.invalid.test");
        assert(ret != NEON_TRUE);
        
    }
    
}
