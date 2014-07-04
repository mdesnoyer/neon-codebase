#ifndef _NEON_EXCEPTION_
#define _NEON_EXCEPTION_

#include <stdarg.h>


class NeonException {
    
public:
    
    static const int MessageSize = 1024;
    
    NeonException(const char * format, ...);
    ~NeonException();
    
    void SetMessage(const char * format, ...);
    const char * GetMessage() const;
    
protected:
    
    //
    char message[MessageSize+1];
};


#endif



