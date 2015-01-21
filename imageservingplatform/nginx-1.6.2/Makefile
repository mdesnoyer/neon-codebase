
default:	build

clean:
	rm -rf Makefile objs

build:
	$(MAKE) -f objs/Makefile
	$(MAKE) -f objs/Makefile manpage

install:
	$(MAKE) -f objs/Makefile install

upgrade:
	/home/pierred/neon-codebase/build/Debug/bin/sbin/nginx -t

	kill -USR2 `cat /home/pierred/neon-codebase/build/Debug/bin/logs/nginx.pid`
	sleep 1
	test -f /home/pierred/neon-codebase/build/Debug/bin/logs/nginx.pid.oldbin

	kill -QUIT `cat /home/pierred/neon-codebase/build/Debug/bin/logs/nginx.pid.oldbin`
