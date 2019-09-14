#!/usr/bin/python3

import sys

data = sys.stdin

venue = None
for line in sys.stdin:
	line = line.strip()
	#line_sep = line.split()
	line_data = line.split(",")
	global venue
	if (line_data[1] == 'venue'):
		if (len(line_data) == 3):
			venue = line_data[2]
		else:
			venue = str(str(line_data[2]) + "," + str(line_data[3]))

	
	if (line_data[0] == 'ball'):
		key_pair = str(venue + "," + str(line_data[4]) + "," + str(line_data[7]))
		print ('%s\t%s' % (key_pair,'1'))

