#!/usr/bin/python3
import sys
import csv
infile = sys.stdin

for row in infile:
    row  = row.strip()
    row = row.split(',')
    #print(row)
    meta = row[0]
    deets = row[1]
    
    #print(row)
    if meta == 'info' and deets == 'venue':
        if(len(row) == 4):
          venue = row[2] + ',' + row[3] 
        else:
          venue = row[2]        
	
      
         
    if meta == 'ball':
        extras = row[8]
        #print(extras,type(extras))
        if(extras == '0'):
            batsman = row[4]
            runs = row[7]
            extras = row[8]
            runs = int(runs)

            print('%s\t%s\t%s\t%s' % (venue,batsman,runs,1))
    
    
