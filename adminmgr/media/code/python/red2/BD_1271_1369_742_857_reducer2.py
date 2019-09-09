#!/usr/bin/python3
import sys
global balls
global wicks
global batter_bowler
final = {}
balls = 0
wicks = 0
batter_bowler = ['','']

	

for line in sys.stdin:
	line = line.strip()
	line_val = line.split(',')
	key = (line_val[0],line_val[1])
	val = (int(line_val[2]),int(line_val[4]))
	try:
		deliveries = val[0]
		wick = val[1]
	except ValueError:
		continue
	
	if batter_bowler[0] == key[0] and batter_bowler[1] == key[1]:
		wicks = wicks + wick
		balls = balls + deliveries
	else:
		if(batter_bowler[0]!='' and batter_bowler[1]!='' and balls > 5):
			final[(batter_bowler[0],batter_bowler[1])]=[wicks,balls]
		balls = 1
		wicks = val[1]
		batter_bowler[0] = key[0]
		batter_bowler[1] = key[1]
if(batter_bowler[0]!='' and batter_bowler[1]!='' and balls > 5):
			final[(batter_bowler[0],batter_bowler[1])]=[wicks,balls]



list1= sorted(final.items())
list2= sorted(list1, key=lambda i: i[1][1])
list3= sorted(list2,key = lambda i: i[1][0], reverse = True)
#print(final)
#for x in final.keys():
	#print(final.keys, final.value)
	#print x[0]+","+x[1]+","+str(final[x][0])+","+str(final[x][1])

for k in list3:
	##print("wickets: "+str(i[1][1]))
	##print(j)
	#print listrun[k][0]
	a,b=k
	c,d=a
	#print (c+","+d+","+str(k[1][0])+","+str(k[1][1])) 
	print ("%s,%s,%d,%d" % (c,d,int(k[1][0]),int(k[1][1])))
