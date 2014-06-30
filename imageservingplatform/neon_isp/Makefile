VPATH = ../nginx-1.4.7
SRC = 



clean:
	rm -f neon_utc_tester neon_utils_tester neon_updater_tester neon_mastermind_tester publisherHashtableTester


all:  clean neon_utc_tester  neon_utils_tester  neon_updater_tester neon_mastermind_tester publisherHashtableTester

neon_utc_tester: neon_utc_tester.c
	gcc -g neon_utc_tester.c neon_utc.c neon_error_codes.c -o neon_utc_tester
    
  
neon_utils_tester: neon_utils_tester.c
	gcc -g neon_utils_tester.c neon_utils.c neon_stats.c neon_utc.c neon_error_codes.c -o neon_utils_tester

    
neon_updater_tester: neon_updater_tester.c
	g++ -g neon_updater_tester.c neon_updater.c neon_stats.c neon_fetch.c neon_utc.c neon_utils.c neon_mastermind.cpp neonHash.cpp mastermind.cpp publisherHashtable.cpp neon_log.c neonLog.cpp directiveHashtable.cpp directive.cpp publisher.cpp fraction.cpp scaledImage.cpp neonException.cpp -lpthread -lstdc++ -o neon_updater_tester

   
neon_mastermind_tester: neon_mastermind_tester.c
	g++ -g neon_mastermind_tester.c neon_mastermind.cpp neonHash.cpp mastermind.cpp publisherHashtable.cpp neon_log.c directiveHashtable.cpp directive.cpp publisher.cpp fraction.cpp scaledImage.cpp neonException.cpp neonLog.cpp -I ../nginx-1.4.7/objs -I ../nginx-1.4.7/src/http/ -I ../nginx-1.4.7/src/core/ -I ../nginx-1.4.7/src/os/unix/ -I ../nginx-1.4.7/src/event/  -I ../nginx-1.4.7/src/mail/  -I ../nginx-1.4.7/src/misc/ -I ../nginx-1.4.7/src/http/modules/ -o neon_mastermind_tester

neon_directive_tester: neon_directive_tester.c
	g++ -g neon_directive_tester.c neon_mastermind.cpp neonHash.cpp mastermind.cpp publisherHashtable.cpp neon_log.c directiveHashtable.cpp directive.cpp publisher.cpp fraction.cpp scaledImage.cpp neonException.cpp neonLog.cpp neon_utils.c neon_stats.c neon_utc.c -I ../nginx-1.4.7/objs -I ../nginx-1.4.7/src/http/ -I ../nginx-1.4.7/src/core/ -I ../nginx-1.4.7/src/os/unix/ -I ../nginx-1.4.7/src/event/  -I ../nginx-1.4.7/src/mail/  -I ../nginx-1.4.7/src/misc/ -I ../nginx-1.4.7/src/http/modules/ -o neon_directive_tester

neon_mastermind_gtest_tester: neon_mastermind_tester_gtest.c
	g++ -g neon_mastermind_tester.c neon_mastermind.cpp neonHash.cpp mastermind.cpp publisherHashtable.cpp neon_log.c directiveHashtable.cpp directive.cpp publisher.cpp fraction.cpp scaledImage.cpp neonException.cpp neonLog.cpp -I ../nginx-1.4.7/objs -I ../nginx-1.4.7/src/http/ -I ../nginx-1.4.7/src/core/ -I ../nginx-1.4.7/src/os/unix/ -I ../nginx-1.4.7/src/event/  -I ../nginx-1.4.7/src/mail/  -I ../nginx-1.4.7/src/misc/ -I ../nginx-1.4.7/src/http/modules/ -o neon_mastermind_gtest_tester

neon_mastermind_tester_producer: neon_mastermind_tester_producer.c
	gcc -g neon_mastermind_tester_producer.c  -o neon_mastermind_tester_producer

publisherHashtableTester: publisherHashtableTester.cpp
	g++ -g publisherHashtableTester.cpp publisherHashtable.cpp  -o publisherHashtableTester


directiveHashtableTester: directiveHashtableTester.cpp
	g++ -g directiveHashtableTester.cpp directiveHashtable.cpp  -o directiveHashtableTester


rapidJsonTester: rapidJsonTester.cpp
	g++ -g rapidJsonTester.cpp  -IrapidJson -o rapidJsonTester

neon_service_test: neon_service_test.cc
	g++ -g neon_service_test.cc neon_service.c neon_mastermind.cpp neonHash.cpp mastermind.cpp publisherHashtable.cpp neon_log.c directiveHashtable.cpp directive.cpp publisher.cpp fraction.cpp scaledImage.cpp neonException.cpp neonLog.cpp neon_utils.c neon_stats.c neon_utc.c -I ../nginx-1.4.7/objs -I ../nginx-1.4.7/src/http/ -I ../nginx-1.4.7/src/core/ -I ../nginx-1.4.7/src/os/unix/ -I ../nginx-1.4.7/src/event/  -I ../nginx-1.4.7/src/mail/  -I ../nginx-1.4.7/src/misc/ -I ../nginx-1.4.7/src/http/modules/ -o neon_directive_tester
