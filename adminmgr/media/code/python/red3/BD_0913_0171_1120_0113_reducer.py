#!/usr/bin/python3
import sys

file_contents = sys.stdin
output_dict = dict()
venue = list()
for line in file_contents:
	LINE = line.strip()
	LIST = LINE.split(":")
	if(LIST[0] not in venue):
		venue.append(LIST[0])
	venue_batsman = LIST[0] + "_" + LIST[1]
	LIST[2] = int(LIST[2])
	if venue_batsman in output_dict.keys():
		runs_sr_balls = output_dict[venue_batsman]
		runs_sr_balls[0] += LIST[2]
		runs_sr_balls[2] += 1		
	else:
		output_dict[venue_batsman] = [LIST[2],0.0,1]
o = list()
for key in output_dict.keys():
	if output_dict[key][2] < 10:
		continue
	output_dict[key][1] = (output_dict[key][0])/(output_dict[key][2])
	venue_batsman = key.split("_")
	o.append(venue_batsman + output_dict[key])

o.sort(key = lambda x: (x[0],-x[3],-x[2]))

for l in o:
	if l[0] not in venue:
		continue
	venue.remove(l[0])
	print(l[0],l[1],sep=",")
