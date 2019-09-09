#!/usr/bin/env python3
"""mapper.py"""

import sys

for line in sys.stdin:
	#line=line.strip()
	line=line.split(',')
	if(not(line[0]=="ball")):
		continue
		'''
	if(line[4]=="YK Pathan" and line[6]=="WD Parnell"):
		print ('%s\t%s\t%s' % (line[4],line[6],line[9]))
		'''
	if(line[9]=="bowled" or line[9]=="caught" or line[9]=="lbw" or line[9]=="caught and bowled" or line[9]=="stumped" or line[9]=="hit wicket" or line [9]=="obstructing the field"):
	#if(not(line[9]==" " or line[9]=="run out")):
		print ('%s\t%s\t%s' % (line[4],line[6],1))
	else:
		print ('%s\t%s\t%s' % (line[4],line[6],0))
