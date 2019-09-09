import csv
from operator import itemgetter
import sys
import operator
finall={}
deliveries=0
wickets=0
bat_bowl=['','']
if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")
for line in sys.stdin:
	global deliveries
	global wickets
	global bat_bowl
	line=line.strip()
	line_val=line.split(",")
	key,val=(line_val[0],line_val[1]),[int(line_val[2]),int(line_val[3])]
	try:
		no_d=val[0]
		no_w=val[1]
	except ValueError:
		continue
	#global deliveries
	#global wickets
	#global bat_bowl
	if bat_bowl[0]==key[0] and bat_bowl[1]==key[1]:
		deliveries+=no_d
		wickets+=no_w
		#print('jus counting')
	else:
		if(bat_bowl[0]!="" and bat_bowl[1]!="" and deliveries>5):
			#print("%s\t%s\t\t\t\t%s\t\t\t%s" % (bat_bowl[0],bat_bowl[1],str(deliveries),str(wickets)))
			finall[(bat_bowl[0],bat_bowl[1])]=[wickets,deliveries]
			#print('changed')
			#print(finall)
		deliveries=1
		wickets=0
		bat_bowl[0]=key[0]
		bat_bowl[1]=key[1]
	#print(wickets,deliveries)
if(bat_bowl[0]!="" and bat_bowl[1]!="" and deliveries>5):
			#print("%s\t%s\t\t\t\t%s\t\t\t%s" % (bat_bowl[0],bat_bowl[1],str(deliveries),str(wickets)))
			finall[(bat_bowl[0],bat_bowl[1])]=[wickets,deliveries]
			#print(final)




#print(finall)
#list=finall.items()
sorted_x=sorted(finall.items(),key =lambda x:(-x[1][0],x[1][1],x[0][0])) #based on Batsman name
#sorted_x = sorted(finall.items(), key=(operator.itemgetter(1)[0],operator.itemgetter(1)[0]))
#print(sorted_x)	
for i in sorted_x:
	#print(%i[0][0],',',i[0][1],',',i[1][0],',',i[1][1]
	print('%s,%s,%s,%s' % (i[0][0],i[0][1],i[1][0],i[1][1]))
