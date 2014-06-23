#include <time.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>

#include "neon_mastermind.h"
#include "neon_log.h"
#include "neon_service.h"
#include "neon_error_codes.h"


int
main(int argc, char ** argv)
{
    
    neon_test_logging = 1;
    // Test URL tokensization
    url = neon_service_get_uri_token();
    
    
}



