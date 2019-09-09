#!/usr/bin/python3
import sys
set={}

for r in sys.stdin:
	r = r.split(",")
	if(r[0] == "ball"):
		b = r[6]
		a= r[4]
		ab = b+"&"+a
		if ab not in set:
			set[ab]=[int(r[7])+int(r[8]),1]
		else:
			set[ab][0]=set[ab][0]+int(r[7])+int(r[8])
			set[ab][1]=set[ab][1]+1

for a in set:
	print("%s$%s"%(a,str(set[a][0])+";"+str(set[a][1])))
