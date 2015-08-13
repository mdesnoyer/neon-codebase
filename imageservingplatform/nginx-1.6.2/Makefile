
default:	build

clean:
	rm -rf Makefile objs

build:
	$(MAKE) -f objs/Makefile
	$(MAKE) -f objs/Makefile manpage

install:
	$(MAKE) -f objs/Makefile install

upgrade:
	/shared/mdesnoyer/src/neon/build/Debug/bin/sbin/nginx -t

	kill -USR2 `cat /shared/mdesnoyer/src/neon/build/Debug/bin/logs/nginx.pid`
	sleep 1
	test -f /shared/mdesnoyer/src/neon/build/Debug/bin/logs/nginx.pid.oldbin

	kill -QUIT `cat /shared/mdesnoyer/src/neon/build/Debug/bin/logs/nginx.pid.oldbin`
