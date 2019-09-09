#!/usr/bin/python3
import sys
import csv


for line in sys.stdin:
	line = line.strip()
	curr_line= line.split(',') #Split the values given in CSV
	dismissals=["lbw","hit wicket","caught","bowled","caught and bowled","stumped","obstructing the field"]
	#This is a list of all the potential ways the batsemnt can get out, so we only increment if the method of getting out is one of these
	#Tried doing =!run out but that gave error 

	if(curr_line[0] == 'ball'):
		out =0
		if(curr_line[9] in dismissals): #Ensuring that the way batsment got out is in our list
			out =1
		bat_bowl_wick=curr_line[4] + ',' + curr_line[6] + ',' + str(out) #Batsment, bowler and flag to check if the ball was a wicket
		print('%s=%s' % (bat_bowl_wick,'1')) #We send this to the reducer, basicaly a triplet with the ball count.
		#else:
			#bat_bowl_wick=curr_line[4] + ',' + curr_line[6] + ',0' #Here flag is 0 as there was no wicket or wicket was run out
			#print('%s\t%s' % (bat_bowl_wick,'1')) #We send this to the reducer, once again a triplet with the ball count

#Kartikeya_Jain, Rohit_Menon, Tarun, Vishnu
