#include <stdio.h>
#include "neonException.h"


NeonException::NeonException(const char * format, ...)
{
    va_list va;
    va_start(va, format);
    
    vsnprintf(message, MessageSize, format, va);
    
    va_end(va);
}


NeonException::~NeonException()
{
}



void
NeonException::SetMessage(const char * format, ...)
{
    va_list va;
    va_start(va, format);
    
    vsnprintf(message, MessageSize, format, va);
    
    va_end(va);
}


const char *
NeonException::GetMessage() const
{
    return message;
}



