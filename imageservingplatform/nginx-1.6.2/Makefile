
default:	build

clean:
	rm -rf Makefile objs

build:
	$(MAKE) -f objs/Makefile
	$(MAKE) -f objs/Makefile manpage

install:
	$(MAKE) -f objs/Makefile install

upgrade:
	/media/nick/d216eb37-b0e1-478c-b170-2270d7699ea2/repos/neon_cv3/neon/build/Release/bin/sbin/nginx -t

	kill -USR2 `cat /media/nick/d216eb37-b0e1-478c-b170-2270d7699ea2/repos/neon_cv3/neon/build/Release/bin/logs/nginx.pid`
	sleep 1
	test -f /media/nick/d216eb37-b0e1-478c-b170-2270d7699ea2/repos/neon_cv3/neon/build/Release/bin/logs/nginx.pid.oldbin

	kill -QUIT `cat /media/nick/d216eb37-b0e1-478c-b170-2270d7699ea2/repos/neon_cv3/neon/build/Release/bin/logs/nginx.pid.oldbin`
