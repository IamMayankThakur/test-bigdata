#!/usr/bin/python
import sys
import csv
data = sys.stdin

venue = None
for line in sys.stdin:
	line = line.strip()
	line_sep = line.split("/t")
	line_data = line_sep[0].split(",")
	global venue
	if (line_data[1] == 'venue'):
		if (len(line_data) == 3):
			venue = line_data[2]
			venu_loc = None
		else:
			venue = line_data[2] 
			venue_loc = line_data[3]
	
	if (line_data[0] == 'ball'):
		if venue_loc is None:
			key_pair = venue + "," + line_data[4] + "," + str(line_data[7])
		else:
			key_pair = venue + "," + venue_loc + "," + line_data[4] + "," + str(line_data[7])	
		print ('%s\t%s' % (key_pair,'1'))
