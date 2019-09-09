#! /usr/bin/python3
import sys
import operator
import collections
dict1 = {}
for line in sys.stdin:
	line = line.strip()
	words= line.split('\t')
	key = words[0]
	batsman_v = words[1]
	runs_v = words[2]
	ball_v = words[3]
	if key not in dict1.keys():
		dict1[key] = [(batsman_v,int(runs_v),int(ball_v))]
	else:		
		i = 0
		entered = 0
		while i < len(dict1[key]) and entered == 0:
			lst = list(dict1[key][i])
			if lst[0] != batsman_v:
				i+=1
			else:
				lst[1] += int(runs_v)
				lst[2] += int(ball_v)
				entered = 1
				dict1[key][i] = tuple(lst)

		if(i==len(dict1[key])):
			dict1[key].append((batsman_v,int(runs_v),int(ball_v)))


for key2 in dict1.keys():
	for i in range(len(dict1[key2])):
		lst2 = list(dict1[key2][i])
		if int(lst2[2])>=10:
			lst2.append(int(lst2[1])*100/int(lst2[2]))	
			dict1[key2][i] = tuple(lst2)
		else:
			lst2.append(-1)
			dict1[key2][i] = tuple(lst2)

for key3 in dict1.keys():
#	s = dict1[key2]
	dict1[key3].sort(key = operator.itemgetter(3, 1),reverse = True)


dict1 = collections.OrderedDict(sorted(dict1.items()))

for f in dict1.keys():
	out2 = f.strip()+","+dict1[f][0][0].strip()
	print(out2)
	#print("%s%s"%(f,dict1[f][0][0]))
	#print("{0},{1}".format(f,dict1[f][0][0]))
