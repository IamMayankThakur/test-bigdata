#!/usr/bin/python3

import sys

dict_red = {}
for line in sys.stdin:
	line = line.strip()
	values = line.split("\t")
	names = values[0].split(",")
	deli = int(values[1])
	if (len(names) == 4):
		key = names[0] + "," + names[1]
		bat = names[2]
		runs = int(names[3])
	else:
		key = names[0]
		bat = names[1]
		runs = int(names[2])
	if key in dict_red:
		if bat in dict_red[key]:
			dict_red[key][bat][0] = int(dict_red[key][bat][0]) + int(runs)
			dict_red[key][bat][1] = int(dict_red[key][bat][1]) + int(deli)
		else:
			dict_red[key][bat] = []
			dict_red[key][bat].append(runs)
			dict_red[key][bat].append(1)
	else:
		dict_red[key] = {}
		dict_red[key][bat] = []
		dict_red[key][bat].append(runs)
		dict_red[key][bat].append(1)
	

for key in list(dict_red):
	for bat in list(dict_red[key]):
		if (dict_red[key][bat][1]<10):
			del dict_red[key][bat]
		else:
			var = dict_red[key].get(bat)
			rate = (float(var[0])*100)/float(var[1])
			dict_red[key][bat].append(rate)


dict_fin = sorted(dict_red.iterkeys())
for key in dict_fin:
	max_run = -1
	for bat in dict_red[key]:
		if (max_run < int(dict_red[key][bat][2])):
			name = bat
			max_run = int(dict_red[key][bat][2])

		elif (max_run == int(dict_red[key][bat][2])):
			if (int(dict_red[key][name][0]) < int(dict_red[key][bat][0])):
				name = bat
	print (key+","+name)
