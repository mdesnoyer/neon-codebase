'''REMOVE lines from target file given lines in fremove '''

import os
import sys

try:
   target = sys.argv[1]
   fremove = sys.argv[2]
except:
   print "./script <target> <links to remove file>"
   sys.exit(1)

rm = open(fremove,'r')
remove_lines = rm.readlines()
tg = open(target,'r')
with open('new_links.txt','w') as f :
    for line in tg.readlines():
        if line not in remove_lines:
            f.write(line)
