#!/usr/bin/python3
import sys

set={}
for a in sys.stdin:
	a = a.split(",")
	if(a[0]=='ball'):
		
		if len(a)>=8:
			batsmanbowler=a[4]+"&"+a[6]
			if batsmanbowler not in set:
				set[batsmanbowler] = [0,1]
				if len(a[9])>2  and a[9] != "run out" and a[9]!="retired hurt":
					set[batsmanbowler][0]=set[batsmanbowler][0]+1
				
			else:
				set[batsmanbowler][1]+=1
				if len(a[9])>2  and a[9] != "run out" and a[9]!="retired hurt":
					set[batsmanbowler][0]=set[batsmanbowler][0]+1
			
for m in set:
	print("%s$%s"%(m,str(set[m][0])+";"+str(set[m][1])))



		
	
		
  
