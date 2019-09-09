#!/usr/bin/python3
#Reducer.py
import sys

pairs = {}
for line in sys.stdin:
	line = line.strip()
	words = line.split("\t");
	a = (words[0],words[1])
	if a not in pairs.keys():
		if words[2]!=" \"\" " and words[2]!=" run out " and words[2]!=" retired hurt ":
			pairs[a] = [1,1]
		else:
			pairs[a] = [0,1]
	else:
		wickets = int(pairs[a][0])
		deliveries = int(pairs[a][1])
		if words[2]!=" \"\" " and words[2]!=" run out " and words[2]!=" retired hurt ":
			pairs[a][0] = wickets + 1
			pairs[a][1] = deliveries + 1
		else:
			pairs[a][1] = deliveries + 1
sort_val = sorted(pairs.items(), key = lambda x : x[1][0],reverse=True)
for m in range(len(sort_val)-1):
   for l in range(len(sort_val)-1):
        tup1= sort_val[l]
        tup2= sort_val[l+1]
        if tup1[1][0] == tup2[1][0]:
            if tup1[1][1] == tup2[1][1]:
                if tup1[0][0] == tup2[0][0]:
                    if tup1[0][1] > tup2[0][1]:
                       temp = sort_val[l]
                       sort_val[l] = sort_val[l+1]
                       sort_val[l+1] = temp
                if tup1[0][0] > tup2[0][0]:
                    temp = sort_val[l]
                    sort_val[l] = sort_val[l+1]
                    sort_val[l+1] = temp
            if tup1[1][1] > tup2[1][1]:
                temp = sort_val[l]
                sort_val[l] = sort_val[l+1]
                sort_val[l+1] = temp

for f in sort_val:
	if int(f[1][1]) > 5:
		print(f[0][0].strip()+","+f[0][1].strip()+","+str(f[1][0])+","+str(f[1][1])+"\t")
	
