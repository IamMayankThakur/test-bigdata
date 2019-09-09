#!/usr/bin/python3
import sys
current_players = None
current_runs = 0
current_balls=0
players = None
dict1={}
for line in sys.stdin:
	line = line.strip()
	venue,players,runs,balls = line.split("_")
	#venue=venue.replace('"',"")
	runs = int(runs)
	balls=int(balls)
	if current_players == players:
		current_runs += runs
		current_balls += balls
	else:
		if current_players:
			if(current_balls>9):
				dict1[venue,current_runs/current_balls]=current_players	
		current_runs = runs
		current_balls=balls
		current_players = players
if current_players == players and current_balls>9:	
	dict1[venue,current_runs/current_balls]=current_players
dict2={}
listofTuples = sorted(dict1.keys())
#print(dict1)
for elem in listofTuples :
	dict2[elem[0]]= [elem[1],dict1[elem]]
for i in dict2.items():
	print(i[0],i[1][1],sep=",")  

