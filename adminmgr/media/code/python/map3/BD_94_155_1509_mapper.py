import sys
import csv
infile = sys.stdin

# i=0
# row = list()
# row.append("yolo")
# row.append("hey")

for line in infile:
   row = (line.strip()).split(',')
   if(len(row)>7):
        i = int(row[7])+ int(row[8]) #runs plus extras
        print(row[6]+","+row[4]+","+str(i)+","+str(1))