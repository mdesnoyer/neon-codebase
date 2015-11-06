
default:	build

clean:
	rm -rf Makefile objs

build:
	$(MAKE) -f objs/Makefile
	$(MAKE) -f objs/Makefile manpage

install:
	$(MAKE) -f objs/Makefile install

upgrade:
	/hdd/repos/neon/build/Debug/bin/sbin/nginx -t

	kill -USR2 `cat /hdd/repos/neon/build/Debug/bin/logs/nginx.pid`
	sleep 1
	test -f /hdd/repos/neon/build/Debug/bin/logs/nginx.pid.oldbin

	kill -QUIT `cat /hdd/repos/neon/build/Debug/bin/logs/nginx.pid.oldbin`
