#include <stdlib.h>
#include <stdio.h>
#include "neon_error_codes.h"
#include "neonLog.h"



int NeonLog::TestLogging = 1;



void
NeonLog::Debug(const char * line, ...)
{
    char message[1024];
    va_list va;
    va_start(va, line);
    vsnprintf(message, 1024, line, va);
    va_end(va);
    
    if(TestLogging)
        LogToTestFile(message);
    
}


void
NeonLog::Info(const char * line, ...)
{
}


void
NeonLog::Notice(const char * line, ...)
{
}


void
NeonLog::Warn(const char * line, ...)
{
}


void
NeonLog::Error(const char * line, ...)
{
}


void
NeonLog::Critical(const char * line, ...)
{
}


void
NeonLog::Alert(const char * line, ...)
{
}


void
NeonLog::Emergency(const char * line, ...)
{
}


void
NeonLog::LogToTestFile(const char * line)
{
    //TODO: Use nginx log file
    static const char * format = "echo \"%s\" >> /tmp/neon_log";
    char command[1024];
    sprintf(command, format, line);
    int ret = system(command);
	if (ret){
	}
}




