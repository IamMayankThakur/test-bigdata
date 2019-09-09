import csv
from operator import itemgetter		#itemgetter is a key function used to used to get the first item from the list-like object
import sys
import operator

#declaration of global variables
global deliveries
global btbw 
global runs


fnl_dic={} 	#final dictionary used to store the final output to be displayed
btbw=['','']	#list used to store the batsman name and the bowler name which becomes the key of the dictionary		
runs=0
deliveries=0

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")
for line in sys.stdin:
	line=line.strip()
	line_val=line.split(",")
	key,val=(line_val[0],line_val[1]),[int(line_val[2]),int(line_val[3])]	#assigning the output of the mapper to the key and value of the dictionary
	try:
		deliveries_no=val[0] 
		runs_no=val[1]
	except ValueError:
		continue

	if((btbw[0]==key[0]) and (btbw[1]==key[1])):
		runs+=runs_no			#counting the total number of runs
		deliveries+=deliveries_no	#counting the total number of balls
		
	else:
		if(btbw[0]!="" and btbw[1]!="" and deliveries>5):
			fnl_dic[(btbw[0],btbw[1])]=[runs,deliveries] 
		runs=0	
		deliveries=1		
		btbw[0]=key[0]
		btbw[1]=key[1]
	#print(wickets,deliveries)

if(btbw[0]!="" and btbw[1]!="" and deliveries>5):
			#print("%s\t%s\t\t\t\t%s\t\t\t%s" % (btbw[0],btbw[1],str(deliveries),str(wickets)))
			fnl_dic[(btbw[0],btbw[1])]=[runs,deliveries]
			#print(final)




#print(fnl_dic)
#list=fnl_dic.items()
sorted_y=sorted(fnl_dic.items(),key =lambda x:(-x[1][0],x[1][1],x[0][1])) #based on bowler name	#fnl_dic.items() returns a list of tuples 
#sorted_x = sorted(fnl_dic.items(), key=(operator.itemgetter(1)[0],operator.itemgetter(1)[0]))
#print(sorted_x)	
for i in sorted_y:
	#print(%i[0][0],',',i[0][1],',',i[1][0],',',i[1][1]
	print('%s,%s,%s,%s' % (i[0][1],i[0][0],i[1][0],i[1][1]))
