#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
import operator
finall={}
deliveries=0
runs=0
bat_bowl=['','']
for line in sys.stdin:
	line=line.strip()
	line_val=line.split("\t")
	keys,val=(line_val[0]),[int(line_val[1]),int(line_val[2])]
	key=keys.split(',')
	no_d=val[1]
	no_r=val[0]
	if bat_bowl[0]==key[0] and bat_bowl[1]==key[1]:
		deliveries+=no_d
		runs+=no_r
		#print('jus counting')
	else:
		if(bat_bowl[0]!="" and bat_bowl[1]!="" and deliveries>5):
			#print("%s\t%s\t\t\t\t%s\t\t\t%s" % (bat_bowl[0],bat_bowl[1],str(deliveries),str(runs)))
			finall[(bat_bowl[0],bat_bowl[1])]=[runs,deliveries]
			#print('changed')
			#print(finall)
		deliveries=1
		runs=no_r
		bat_bowl[0]=key[0]
		bat_bowl[1]=key[1]
	#print(wickets,deliveries)
if(bat_bowl[0]!="" and bat_bowl[1]!="" and deliveries>5):
			#print("%s\t%s\t\t\t\t%s\t\t\t%s" % (bat_bowl[0],bat_bowl[1],str(deliveries),str(wickets)))
			finall[(bat_bowl[0],bat_bowl[1])]=[runs,deliveries]
			#print(final)
#print(finall)
#list=finall.items()
sorted_x=sorted(finall.items(),key =lambda x:(-x[1][0],x[1][1],x[0][0]))
#sorted_x = sorted(finall.items(), key=(operator.itemgetter(1)[0],operator.itemgetter(1)[0]))
#print(sorted_x)	
for i in sorted_x:
	#print(%i[0][0],',',i[0][1],',',i[1][0],',',i[1][1]
	print('%s,%s,%d,%d' % (i[0][0],i[0][1],i[1][0],i[1][1]))
	#print( i[0][1],',',i[0][0],',',i[1][0],',',i[1][1])


