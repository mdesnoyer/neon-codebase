#include <stdlib.h>
#include <stdio.h>
#include "neon_log.h"


int test_logging = 1;


static void
log_to_test_file(const char * line)
{
    static const char * format = "echo \"%s\" >> /tmp/neon_log";
    char command[1024];
    sprintf(command, format, line);
    int ret = system(command);
    if (ret){
    }
}


void
neon_log_debug(const char * line, ...)
{
    
}


void
neon_log_info(const char * line, ...)
{
    
}


void
neon_log_notice(const char * line, ...)
{
    
}


void
neon_log_warn(const char * line, ...)
{
    
}


void
neon_log_error(const char * line, ...)
{
    char message[1024];
    va_list va;
    va_start(va, line);
    vsnprintf(message, 1024, line, va);
    va_end(va);
    
    if(test_logging)
        log_to_test_file(message);
}


void
neon_log_critical(const char * line, ...)
{
    
}


void
neon_log_alert(const char * line, ...)
{
    
}


void
neon_log_emergency(const char * line, ...)
{
    
}




