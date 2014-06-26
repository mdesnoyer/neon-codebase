#include <assert.h>
//#include <gtest/gtest.h>
#include <limits.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "directive.h"
#include "neon_error_codes.h"
#include "neon_mastermind.h"
#include "neon_log.h"
#include "neon_updater.h"
#include "neon_utils.h"

//// Tests /////

// Directive Parsing, test json document
void
test_directive_parsing(){

	char dir[] = "{\"type\":\"dir\",\"aid\":\"acc0\",\"vid\":\"vid0\",\"sla\":\"expiry=2014-06-13T00:27:29Z\",\"fractions\":[{\"pct\":0.7,\"default_url\":\"http://d_url\",\"tid\":\"thumb1\",\"imgs\":[{\"h\":500,\"w\":600,\"url\":\"http://neon/thumb1_500_600.jpg\"},{\"h\":700,\"w\":800,\"url\":\"http://neon/thumb2_700_800.jpg\"}]},{\"pct\":0.2,\"default_url\":\"http://d_url\",\"tid\":\"thumb2\",\"imgs\":[{\"h\":500,\"w\":600,\"url\":\"http://neont2/thumb1_500_600.jpg\"},{\"h\":300,\"w\":400,\"url\":\"http://neont2/thumb2_300_400.jpg\"}]},{\"pct\":0.1,\"default_url\":\"http://d_url\",\"tid\":\"thumb3\",\"imgs\":[{\"h\":500,\"w\":600,\"url\":\"http://neont3/thumb1_500_600.jpg\"},{\"h\":300,\"w\":400,\"url\":\"http://neont3/thumb2_300_400.jpg\"}]}]}";
    rapidjson::Document document;
    document.Parse<0>(dir);
	Directive * d = new Directive();
	d->Init(document);
	assert(strcmp("acc0", d->GetAccountId()) == 0);
}


// Get Fraction
void
test_get_fraction(){
	char dir[] = "{\"type\":\"dir\",\"aid\":\"acc0\",\"vid\":\"vid0\",\"sla\":\"expiry=2014-06-13T00:27:29Z\",\"fractions\":[{\"pct\":0.7,\"default_url\":\"http://d_url\",\"tid\":\"thumb1\",\"imgs\":[{\"h\":500,\"w\":600,\"url\":\"http://neon/thumb1_500_600.jpg\"},{\"h\":700,\"w\":800,\"url\":\"http://neon/thumb2_700_800.jpg\"}]},{\"pct\":0.2,\"default_url\":\"http://d_url\",\"tid\":\"thumb2\",\"imgs\":[{\"h\":500,\"w\":600,\"url\":\"http://neont2/thumb1_500_600.jpg\"},{\"h\":300,\"w\":400,\"url\":\"http://neont2/thumb2_300_400.jpg\"}]},{\"pct\":0.1,\"default_url\":\"http://d_url\",\"tid\":\"thumb3\",\"imgs\":[{\"h\":500,\"w\":600,\"url\":\"http://neont3/thumb1_500_600.jpg\"},{\"h\":300,\"w\":400,\"url\":\"http://neont3/thumb2_300_400.jpg\"}]}]}";
    
	rapidjson::Document document;
    document.Parse<0>(dir);
	Directive * d = new Directive();
	d->Init(document);

	// Valid fraction
	ngx_str_t hs = ngx_string("12345");
	const Fraction *f = d->GetFraction(hs.data, hs.len);
	assert(f != 0);	
	
	// Get a valid fraction with null hash_string
	f = d->GetFraction(0, 0);
	assert(f != 0);	
	assert(f->GetPct() >= 0.7); //Assert that you got the majority fraction

	// Check the distribution of AB bucket selection
	int i = 0;
	char r_str[8];
	for(; i<100; i++){
		neon_get_uuid(r_str, 8);
		f = d->GetFraction((unsigned char*)r_str, 8);
		printf("%f \n", f->GetPct());
	}
}


// Test hash function
void
test_hash_function(){
	Directive * d = new Directive();
	unsigned char *str = 0;
	unsigned long r = d->neon_sdbm_hash(str, 20);
	assert(r == 0);

	char *str2 = "some random string";
	r = d->neon_sdbm_hash((unsigned char*)str2, sizeof(str2)-1);
	assert(r != 0);
}

//TEST(HF, st){
//	EXPECT_EQ(0, test_hash_function);
//}

int
main(int argc, char ** argv)
{
    
   	// Mastermind Setup 
	
    char * mastermind = "test/mastermind";
    
    NEON_BOOLEAN b = neon_mastermind_init();
    NEON_LOAD_ERROR err = neon_mastermind_load(mastermind);
    assert(err == NEON_LOAD_OK);

	// Tests
	test_directive_parsing();
	test_hash_function();
	test_get_fraction();
	
}



