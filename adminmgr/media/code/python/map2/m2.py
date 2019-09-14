#!/usr/bin/python3
#batsman vulnerability to bowler
import sys
import csv
infile = sys.stdin
next(infile)
# input comes from STDIN (standard input)
# Mapper
d = {}
final_list = []
for line in infile:
    #line = line.strip()
    my_list = line.split(",")
    runs = ""
    if (my_list[0] == "ball"):
        try:
        	if (len(my_list[0]) != 0):
        		runs = int(my_list[7]) + int(my_list[8])
        except:
        	pass
        pair = my_list[6]+","+my_list[4]
        if pair not in d:
        	d[pair] = str(runs)
        else:
        	d[pair] += " " + str(runs)
    else:
    	continue
    #if (my_list[0]=="info"):
     #   continue
    #pair = my_list[4]+","+my_list[6]
    #if pair not in d:
    #    d[pair] = s
    #else:
    #    d[pair] += " " + s
for key in d.keys():
    final_list.append(key+"\t"+d[key])
        
final_list.sort()
#for i in final_list:
#	sys.stderr.write(i+"\n")

for i in final_list:
	totalRuns = 0
	totalDeliveries = 0
	data = i.split("\t")
	runs = data[1].split(" ")
	totalDeliveries=len(runs)
	for j in runs:
		totalRuns += int(j)
	if(totalDeliveries > 5):
		temp=data[0]+"\t"+str(totalRuns)+","+str(totalDeliveries)
		print(temp)
