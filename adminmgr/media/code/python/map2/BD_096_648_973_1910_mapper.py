#!/usr/bin/env python3
"""mapper.py"""

import sys

for line in sys.stdin:
	#line=line.strip()
	line=line.split(',')
	if(not(line[0]=="ball")):
		continue
	print ('%s\t%s\t%s\t%s' % (line[4],line[6],line[7],line[8]))
