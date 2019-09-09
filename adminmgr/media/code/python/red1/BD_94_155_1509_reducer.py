#!/usr/bin/python3
import sys

final = dict()
try_1 = dict()


for line in sys.stdin: #each line
	line = line.strip() #removes whitespaces
	line_val = line.split(",") #splits each line into values
	k1,k2,k3,k4 = line_val[0], line_val[1],int(line_val[2]),int(line_val[3])

	if((k1,k2) not in final.keys()):
		final[k1,k2] = (k3,k4)

	else:
		add_values = final[k1,k2]
		final[k1,k2] = (k3 + add_values[0] , k4+add_values[1])

for key,value in list(final.items()):
	if(value[1] >= 6):
		pass
	else:        
		del final[key]

final_list=sorted(final.items(),key = lambda x:(-x[1][0] , x[1][1]))
for ele in range(len(final_list)):
	print("%s,%s,%d,%d"%(final_list[ele][0][0],final_list[ele][0][1],final_list[ele][1][0],final_list[ele][1][1]))

