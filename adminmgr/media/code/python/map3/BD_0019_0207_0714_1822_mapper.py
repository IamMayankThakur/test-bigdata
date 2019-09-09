#!/usr/bin/python3
import sys
import csv
i = 0
stadium=""
infile_third = sys.stdin
for line in infile_third: #splits into rows
    line_array = line.strip() 
    my_list = line_array.split(',') #splits row into params
    i = i+1
    if(len(my_list)< 7): #skips lines abt match details
    	if(my_list[1]=='venue'):
	    if(my_list[2]=='M.Chinnaswamy Stadium'):
		stadium='M Chinnaswamy Stadium'
	    else:	
	        stadium=my_list[2]
	        stadium=stadium.replace('"','')
   #else:
    	#print(len(my_list[10]))
    	#if(my_list[10]!= '""'):
    #elif(my_list[9] == 'run out' or my_list[9] == '""'):
    			#print(my_list)
    	#print('%s,%s,%d,%d' % (my_list[5],my_list[6],0,1))          #wicket,balls
    else:
	 j=int(my_list[8])+int(my_list[7])
    	 print('%s,%s,%d,%d' % (stadium,my_list[4],j,1))         #wicket,balls



