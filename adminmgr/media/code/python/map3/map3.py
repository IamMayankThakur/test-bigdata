#!/usr/bin/python3
import sys
infile = sys.stdin

for line in infile:
	line = line.strip()
	m_l = line.split('.')
	if m_l[0] ! = "info" and m_l[0] !="version":
	bowl = m_l[6]
	bat = m_l[4]
	extra = m_l[8]
	runs = int(runs)
	extra =int(extras)
	print('%s,%s,%d,%d'%(bowl,bat,runs+extra,1))
