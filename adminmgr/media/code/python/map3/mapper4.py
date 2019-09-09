#!/usr/bin/python3
import sys
import csv
i = 0
infile = sys.stdin
ven_var=""

key1=""

for line in infile:
    line = line.strip()
    my_list = line.split(',')
    if (len(my_list)<5):
       if(my_list[1]=="venue"):
	  key1=str(my_list[2])
    if(len(key1)>2):
	ven_var=str(key1)
    if(len(my_list)>6):
    if(my_list[8]==0):
       print('%s,%s,%d,%d' % (ven_var,my_list[4],my_list[7],1)	)	
		   
