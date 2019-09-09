#!/usr/bin/python3
import sys
import csv
i = 0
infile = sys.stdin
for line in infile:
    line = line.strip()
    my_list = line.split(',')
    i = i+1
    #print(my_list)
    #if(i == 50):
    #	break
    #print((my_list))
    if(len(my_list)< 7):
    	continue
   #else:
    	#print(len(my_list[10]))
    	#if(my_list[10]!= '""'):
    elif(my_list[9] == 'run out' or my_list[9] == '""' or my_list[9] == 'retired hurt'):
    			#print(my_list)
    	print('%s,%s,%d,%d' % (my_list[4],my_list[6],0,1))          #wicket,balls
    else:
    	 print('%s,%s,%d,%d' % (my_list[10],my_list[6],1,1))         #wicket,balls



