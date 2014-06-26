#ifndef _NEON_PARSE_
#define _NEON_PARSE_

#include <stdio.h>
#include "neon_error_codes.h"
#include "neon_mastermind.h"


/*
 *
 */
typedef enum  {
    
    NEON_PARSE_OK = 0,
    NEON_PARSE_MEMORY_EXCEEDED
    
} NEON_PARSE_ERROR;


extern const char * neon_parse_error;


NEON_PARSE_ERROR neon_parse(FILE * mastermind_file,
                           Mastermind * candidate);


#endif


