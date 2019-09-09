#!/usr/bin/python3
import sys
import csv
infile = sys.stdin
c=''
for line in infile:
    line = line.strip()
    my_list = line.split(',')
    if(my_list[0]=='info' and my_list[1]=='venue'):
      try:#Checking for any string after the name of the stadium like name etc.
        c=my_list[2]+","+my_list[3]
      except:
        c=my_list[2]
    while(my_list[0]=='ball' and my_list[8]=='0'):#Not considering any extras 
        #tup=(c,my_list[4],int(my_list[7]),1)
        print('%s\t%s\t%d\t%d'%(c,my_list[4],int(my_list[7]),1))
        break
#Result sent to the Reducer in the format:
#(Venue,Player Name,Run_per_ball,Ball(1))
