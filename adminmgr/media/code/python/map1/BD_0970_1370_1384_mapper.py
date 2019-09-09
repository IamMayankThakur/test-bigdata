#!/usr/bin/python3
import sys

wickets={}
for line in sys.stdin:
	line = line.strip()
	line = line.split(",")
	if(line[0]=='ball'):
		
		if len(line)>=8:
			bat_bowl=line[4]+"/"+line[6]
			if bat_bowl not in wickets:
				wickets[bat_bowl] = [0,1]
				if len(line[9])>2  and line[9] != "run out" and line[9]!="retired hurt":
					wickets[bat_bowl][0]+=1
				
			else:
				wickets[bat_bowl][1]+=1
				if len(line[9])>2  and line[9] != "run out" and line[9]!="retired hurt":
					wickets[bat_bowl][0]+=1
			
for l in wickets:
	print("%s\t%s"%(l,str(wickets[l][0])+"/"+str(wickets[l][1])))



		
	
		
  
