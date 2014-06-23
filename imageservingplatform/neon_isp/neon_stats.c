#include <stdlib.h>
#include "neon_stats.h"



unsigned long long int neon_stats[NEON_STATS_NUM_OF_ELEMENTS];


void neon_stats_init()
{
    int i=0;
    for(; i < NEON_STATS_NUM_OF_ELEMENTS; i++)
        neon_stats[i] = 0;
     
}
