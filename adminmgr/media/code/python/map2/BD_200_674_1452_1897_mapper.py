#!/usr/bin/env python3

import sys

for line in sys.stdin:
	line=line.strip()
	words = line.split(",")
	if words[0]=="ball":
		print(words[4],'\t',words[6],'\t',words[7],'\t',1,'\t',words[8])
	
