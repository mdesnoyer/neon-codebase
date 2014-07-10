#ifndef _NEON_STATS_
#define _NEON_STATS_

#include "neon_error_codes.h"


/*
 *  Stats counters name/index into array
 *
 *  To add a stat, declare the ENUM before NEON_STATS_NUM_OF_ELEMENTS,
 *  And use that as index in to the neon_stats array
 */
typedef enum {

	// Mastermind stats	
	MASTERMIND_FILE_FETCH_SUCCESS = 0,
	MASTERMIND_FILE_FETCH_FAIL,

	MASTERMIND_PARSE_SUCCESS,
	MASTERMIND_PARSE_FAIL,

	MASTERMIND_RENAME_SUCCESS,
	MASTERMIND_RENAME_FAIL,

	// Neon Service
	NEON_SERVICE_TOKEN_FAIL, // failed to parse token
	NEON_SERVICE_TOKEN_NOT_FOUND,
	NEON_SERVICE_COOKIE_PRESENT,
	NEON_SERVICE_COOKIE_SET,
	NEON_SERVICE_COOKIE_SET_FAIL,

	// Client Image server stats
	NEON_CLIENT_API_ACCOUNT_ID_NOT_FOUND,
	NEON_CLIENT_API_URL_NOT_FOUND,

	// Server Image server stats
	NEON_SERVER_API_ACCOUNT_ID_NOT_FOUND,
	NEON_SERVER_API_URL_NOT_FOUND,

    // Neon updater stats
    NEON_UPDATER_HTTP_FETCH_FAIL,
    NEON_UPDATER_MASTERMIND_EXPIRED,
    NEON_UPDATER_MASTERMIND_LOAD_FAIL,
    NEON_UPDATER_MASTERMIND_RENAME_FAIL,

    // Neon request stats
    NEON_SERVER_API_REQUESTS,
    NEON_CLIENT_API_REQUESTS,
    NEON_GETTHUMBNAIL_API_REQUESTS,

    // Out of memory counter ?
    NGINX_OUT_OF_MEMORY,

	NEON_STATS_NUM_OF_ELEMENTS
} NEON_STATS;


/*
 *  Stats counters array
 */
extern unsigned long long int neon_stats[];


/*
 *  Zeros all counters
 */
void neon_stats_init();


#endif





