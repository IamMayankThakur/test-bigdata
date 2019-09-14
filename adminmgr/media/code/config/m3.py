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
count =0
#count2=0
flag=0
for line in infile:
	#count2+=1;
	#if(count2==536):
	#	break
	my_list = line.split(",")
	#print(my_list[0])
	if(my_list[0]=="version"):
		continue;
	elif (my_list[0]=="info" and my_list[1]=="venue"):
		#count+=1;
		#print(count)
		#if(count!=8):
	  	#	continue;
		#else:
	  	#count=0;
	  	#flag=1
	  	#print(my_list)
	  	if(len(my_list) > 3):
	  		venue = my_list[2] + "," + my_list[3]
	  	else:
	  		venue = my_list[2]
	  	venue = venue.replace('\r', '').replace('\n', '')
		  	#print(venue)
	elif (my_list[0] == "ball" and my_list[8]=="0"):
		#try:
		flag=0
		pair = venue + "|" + my_list[4]
		#print(pair)
		if pair not in d :
			d[pair] = [int(my_list[7]),1]
			#print(d[pair])
		else :
			#if(pair=="SuperSport Park|LRPL Taylor"):
			#	print("1",d[pair])
			temp = d[pair]
			d[pair] = [int(temp[0]) + int(my_list[7]), int(temp[1]) + 1]
			#if(pair=="SuperSport Park|LRPL Taylor"):
			#	print("2",d[pair])

for key in d.keys():
	temp = key.split("|")
	#if(key=="SuperSport Park|LRPL Taylor"):
	#	print(str(d[key][0])+ str(d[key][1]))
	final_list.append( temp[0] + "\t" +temp[1] + "\t" + str(d[key][0])+ "\t" + str(d[key][1]))
        
final_list.sort()
#for i in final_list:
#	sys.stderr.write(i+"\n")

for i in final_list:
	data = i.split("\t")
	#print(data[2][1])
	if(int(data[3])>=10):
		temp = data[0] +"\t" + data[1]  + "," + str(data[2]) + "," + str(data[3])
		temp=temp.replace(r'\n',"")
		print(temp)
	'''vals = data[1].split(" ")
	totalDeliveries=len(vals)
	for j in vals:
		if (j == "1"):
			totalWickets += 1
	if(totalDeliveries > 1):
		temp=data[0]+"\t"+str(totalWickets)+","+str(totalDeliveries)
		print(temp)'''
