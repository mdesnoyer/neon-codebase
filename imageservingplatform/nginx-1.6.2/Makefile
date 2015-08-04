
default:	build

clean:
	rm -rf Makefile objs

build:
	$(MAKE) -f objs/Makefile
	$(MAKE) -f objs/Makefile manpage

install:
	$(MAKE) -f objs/Makefile install

upgrade:
	/home/mdesnoyer/src/neon/build/Debug/bin/sbin/nginx -t

	kill -USR2 `cat /home/mdesnoyer/src/neon/build/Debug/bin/logs/nginx.pid`
	sleep 1
	test -f /home/mdesnoyer/src/neon/build/Debug/bin/logs/nginx.pid.oldbin

	kill -QUIT `cat /home/mdesnoyer/src/neon/build/Debug/bin/logs/nginx.pid.oldbin`
