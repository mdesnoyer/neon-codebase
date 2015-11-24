
default:	build

clean:
	rm -rf Makefile objs

build:
	$(MAKE) -f objs/Makefile
	$(MAKE) -f objs/Makefile manpage

install:
	$(MAKE) -f objs/Makefile install

upgrade:
	/repos/neon_cv3/neon/build/Release/bin/sbin/nginx -t

	kill -USR2 `cat /repos/neon_cv3/neon/build/Release/bin/logs/nginx.pid`
	sleep 1
	test -f /repos/neon_cv3/neon/build/Release/bin/logs/nginx.pid.oldbin

	kill -QUIT `cat /repos/neon_cv3/neon/build/Release/bin/logs/nginx.pid.oldbin`
