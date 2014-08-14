#ifndef _NEON_LOG_C_
#define _NEON_LOG_C_


#include <stdarg.h>

extern int test_logging;

void neon_log_debug(const char * line, ...);
void neon_log_info(const char * line, ...);
void neon_log_notice(const char * line, ...);
void neon_log_warn(const char * line, ...);
void neon_log_error(const char * line, ...);
void neon_log_critical(const char * line, ...);
void neon_log_alert(const char * line, ...);
void neon_log_emergency(const char * line, ...);


#endif

