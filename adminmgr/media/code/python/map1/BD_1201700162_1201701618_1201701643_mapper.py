#!/usr/bin/python3
import sys
import string



for line in sys.stdin:
	words = line.split(",")
	if words[0]=="ball":
		key=words[4]+","+words[6]
		words[9]=words[9].replace(" ","")
		if(words[9]=="\"\"" or words[9]=="retiredhurt" or words[9]=="runout"):
			a=0
		else:
			a=1
		print(key,a,1,sep=",")
			
		
				
	
				
			
		
	
