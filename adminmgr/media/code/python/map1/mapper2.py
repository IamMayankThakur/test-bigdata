#!/usr/bin/python3
"""mapper.py"""

import sys
for line in sys.stdin:
	line = line.strip()
	words = line.split(",")
	if(words[0]=='ball'):
		print(words[4],'\t',words[6],'\t',words[9],'\t',1)
		

