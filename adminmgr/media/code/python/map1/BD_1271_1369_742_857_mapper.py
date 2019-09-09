#!/usr/bin/python3

import sys
read_line = sys.stdin
out=0
for line in read_line:
	line = line.strip();
	my_list = line.split(',')
	if(my_list[0]=='ball'):
		if(my_list[9] == 'run out' or my_list[9] == 'retired hurt' or my_list[9] == '""'):
			out = 0
		else:
			out = 1
		batsman = my_list[4]
		bowler = my_list[6]
		runs = int(my_list[7]) + int(my_list[8])
		record = (batsman, bowler, 1, runs, out)
		print('%s,%s,%s,%s,%s' % (bowler,batsman, 1, runs, out))
		
