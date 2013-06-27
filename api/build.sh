python -m py_compile *.py
if [ $? -eq 0 ]
	then x=$(git rev-list HEAD | wc -l)
	echo $x > code.version
	tar cvzf neon-code.tar.gz --exclude={lib,result,results,*.pyc,build.sh,*.mov,README*} *
fi

