#!usr/bin/python3

from operator import itemgetter
import sys
final = {}
balls = 0
runs = 0
batter_bowler = ['','']
if not sys.warnoptions:
	import warnings
	warnings.simplefilter('ignore')
	

for line in sys.stdin:
	line = line.strip()
	line_val = line.split(',')
	key = (line_val[0],line_val[1])
	val = (int(line_val[2]),int(line_val[3]))
	try:
		deliveries = val[0]
		run = val[1]
	except ValueError:
		continue
	global balls
	global runs
	global batter_bowler
	if batter_bowler[0] == key[0] and batter_bowler[1] == key[1]:
		runs = runs + run
		balls = balls + deliveries
	else:
		if(batter_bowler[0]!='' and batter_bowler[1]!='' and balls > 5):
			final[(batter_bowler[0],batter_bowler[1])]=[runs,balls]
		balls = 1
		runs = val[1]
		batter_bowler[0] = key[0]
		batter_bowler[1] = key[1]
if(batter_bowler[0]!='' and batter_bowler[1]!='' and balls > 5):
			final[(batter_bowler[0],batter_bowler[1])]=[runs,balls]

list1 = sorted(final.iteritems())
list2 = sorted(list1, key = lambda i: i[1][1])
list3 = sorted(list2, key = lambda i: i[1][0], reverse = True)

#print(final)
#for x in final.keys:
	#print(final.keys, final.value)
	#print x[1]+","+x[0]+","+str(final[x][0])+","+str(final[x][1])
for k in list3:
	##print("wickets: "+str(i[1][1]))
	##print(j)
	a,b=k
	c,d=a
	print (c+","+d+","+str(k[1][0])+","+str(k[1][1])) 



