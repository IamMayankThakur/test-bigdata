#!/usr/bin/python3
import sys

set={}
x="run out"
y="retired hurt"
z='\"\"'
for a in sys.stdin:
	a = a.split(",")
	if(len(a)>4):
		
			batsmanbowler=a[4]+"&"+a[6]
			if batsmanbowler not in set:
				set[batsmanbowler] = [0,0]
				if a[9]!=z  and a[9] !=x and a[9]!=y:
					set[batsmanbowler][0]=set[batsmanbowler][0]+1
				set[batsmanbowler][1]=set[batsmanbowler][1]+1
				
			else:
				set[batsmanbowler][1]+=1
				if a[9]!=z  and a[9] !=x and a[9]!=y:
					set[batsmanbowler][0]=set[batsmanbowler][0]+1

			
for m in set:
	a=str(set[m][1])
	b=str(set[m][0])
	c=b+";"+a
	print("%s$%s"%(m,c))



		
	
		
  
