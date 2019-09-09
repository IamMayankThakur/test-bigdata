#!/usr/bin/python3
"""reducer.py"""

from operator import itemgetter
import sys

current_batsman = None
current_count = 0
current_run=0
word = None
current_venue=None
max_strate=0
best_batsman=None
best_runs=0
d={}
# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
	line = line.strip()

    # parse the input we got from mapper.py
	pair,runs,count = line.split('\t')
	venue,batsman=pair.split('*')
    #print "%s\t%s\t%s" % (best_batsman,venue,max_strate)
    # convert count (currently a string) to int
	try:
		count = int(count)
		runs=int(runs)
	except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
		continue
	if venue not in d.keys():
		d[venue]={}
		if batsman not in d[venue].keys():
			d[venue][batsman]=[runs,count]
	else:
		if batsman not in d[venue].keys():
			d[venue][batsman]=[runs,count]		
		else:
			d[venue][batsman][0]+=runs
			d[venue][batsman][1]+=count
answer={}
for i in d.keys():
	for j in d[i].keys():	
		d[i][j].append((float(d[i][j][0])*100)/float(d[i][j][1]))

for i in d.keys():
	max_str=0
	best_runs=0
	for j in d[i].keys():
		if(d[i][j][1] >= 10):
			if(max_str==d[i][j][2] and best_runs<d[i][j][0]):
				max_str=d[i][j][2]
				best_runs=d[i][j][0]
				answer[i]=j
			elif(max_str<d[i][j][2]):
				max_str=d[i][j][2]
				best_runs=d[i][j][0]
				answer[i]=j
			else:
				pass
ans = sorted(answer.items(), key = lambda x:x[0])
for i in ans:
	print(i[0], i[1], sep=",") 
		

			 
			
		


