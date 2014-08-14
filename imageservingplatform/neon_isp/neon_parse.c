#include "neon_utils.h"
#include "neon_stats.h"
#include "neon_log.h"
#include "neon_parse.h"


const char * neon_parse_error;


NEON_PARSE_ERROR
neon_parse(FILE * mastermind_file,
           Mastermind * candidate)
{
    
    neon_log(NEON_DEBUG, "neon_parse");
    candidate->expiry = neon_get_expiry_from_file(mastermind_file);
    
    neon_parse_error = STR(NEON_PARSE_OK);
    
    neon_stats[MASTERMIND_PARSE_SUCCESS]++;
    
    return NEON_PARSE_OK;
}

