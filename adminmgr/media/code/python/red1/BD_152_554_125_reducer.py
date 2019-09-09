#!/usr/bin/python3
import csv
from operator import itemgetter
import sys

#current_count = 0
#current_key = ""
current_key = ""
current_wickets = 0
current_balls = 0
results = []
for line in sys.stdin:
	line = line.strip()
	line_val = line.split(",")
	bat, bowl, wickets, balls = line_val[0], line_val[1], line_val[2], line_val[3]
	key = bat + "," + bowl
	if current_key == key:
		current_wickets += int(wickets)
		current_balls += int(balls)
	else:
		if (current_key != ""):
			value = str(current_wickets) + "," + str(current_balls)
			if(current_balls > 5):
				results.append([current_key, current_wickets, current_balls])
			current_wickets = int(wickets)
			current_balls = int(balls)
			current_key = key
		else:
			current_key = key
			current_wickets = int(wickets)
			current_balls = int(balls)

if current_key == key:
    value = str(current_wickets) + "," + str(current_balls)
    if(current_balls > 5):
        results.append([current_key, current_wickets, current_balls])

results = sorted(results, key = lambda x: (-x[1],x[2],x[0]))

for i in range(len(results)):
	print("%s,%s,%s" % (results[i][0], str(results[i][1]), str(results[i][2])))

 




