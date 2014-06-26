#ifndef _NEON_LOG_CPP_
#define _NEON_LOG_CPP_

#include "stdarg.h"


class NeonLog {

    
public:
    
    // set to 1 if not using nginx logging, like for debugging
    static int TestLogging;

    static void Debug(const char * line, ...);
    static void Info(const char * line, ...);
    static void Notice(const char * line, ...);
    static void Warn(const char * line, ...);
    static void Error(const char * line, ...);
    static void Critical(const char * line, ...);
    static void Alert(const char * line, ...);
    static void Emergency(const char * line, ...);
    
    
protected:

    static void LogToTestFile(const char * line);
    
};


#endif

