#!/usr/bin/python3
import csv
import sys
current_count = 0
current_key = ""
current_count = 0
current_key = ""
codyko = dict()
i = 0
for line in sys.stdin:
    line = line.strip()
    line_val = line.split(",")
    key1,key2,key3,key4 = line_val[0], line_val[1],int(line_val[2]),int(line_val[3])

    if((key1,key2) in codyko.keys()):
        d = codyko[key1,key2]
        codyko[key1,key2] = (key3 + d[0] , key4+d[1]) 
    else:
        codyko[key1,key2] = (key3,key4)


for k,v in list(codyko.items()):
    v1,v2 = v[0],v[1]
    #print(v1,v2)
    if(v[1] < 6):
        del codyko[k]
old = []
for i in sorted(codyko.items(),reverse = True,key = lambda x:x[1]) :
    old.append(i)
i = 0
j = 0
for k in range(len(old)-1):
  if(old[k][1][0] == old[k+1][1][0]):
    pass
  elif(old[k][1][0] != old[k+1][1][0]):
    old[i:k+1] = sorted(old[i:k+1],reverse = False,key =  lambda x:x[1][1])
    i = k+1
  new_i = i



old[new_i:k+2] = sorted(old[new_i:k+2],reverse = False,key =  lambda x:x[1][1])

for i in old:
    print("%s,%s,%d,%d"%(i[0][0],i[0][1],i[1][0],i[1][1]))
    #print(codyko[i],end = "\n")
    #break
    #print(i[0][0],end = ",")
    #print(i[0][1],end = ",")
    #print(i[1],end = "\n")
    #print(,end = "\n")

'''if((key1,key2) in codyko.keys()):
    	codyko[key1,key2] = codyko[key1,key2] + 1
    else:
    	codyko[key1,key2] = 1
#print(codyko)
for i in sorted(codyko.items(),reverse = True,key = lambda x:x[1]) :
    #print(i[0],end=",")
    #print(i[1],end=",")
    #print(codyko[i],end = "\n")
	#break
    print(i[0][0],end = ",")
    print(i[0][1],end = ",")
    print(i[1],end = "\n")
    #print(,end = "\n")

'''