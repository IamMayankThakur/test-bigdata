#1/usr/bin/python3

import sys 
venue = ""
ven_lis = []
for line in sys.stdin:
    line = line.strip()
    if(line.split(",", 2)[1] == "venue"):
       line = line.split(",", 2)
    else:
       line = line.split(",")
    try:
	if(int(line[8]) != 0):
	    continue
        print(str([venue, line[4], int(line[7]), 1]))
    except IndexError:
	if (line[1] == "venue"):
            venue = line[2]
