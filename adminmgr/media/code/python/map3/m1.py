#!/usr/bin/python
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
    s = ""
    if (my_list[0] == "ball"):
        try:
        	if (len(my_list[0]) != 0 and (my_list[9][0] == 'b' or my_list[9][0] == 'c' or my_list[9][0]=='l')):
        		s = "1"
        except:
        	s="0"
        	pass
        pair = my_list[4]+","+my_list[6]
        if pair not in d:
		    d[pair] = s
        else:
		    d[pair] += " " + s
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
	totalWickets = 0
	totalDeliveries = 0
	data = i.split("\t")
	vals = data[1].split(" ")
	totalDeliveries=len(vals)
	for j in vals:
		if (j == "1"):
			totalWickets += 1
	if(totalDeliveries > 5):
		temp=data[0]+"\t"+str(totalWickets)+","+str(totalDeliveries)
		print(temp)
