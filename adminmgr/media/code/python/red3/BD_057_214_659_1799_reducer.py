import sys
import csv
from operator import itemgetter

dict_red = {}
for line in sys.stdin:
	line = line.strip()
	values = line.split("\t")
	names = values[0].split(",")
	deli = int(values[1])
	if (len(names) == 4):
		key = names[0] + "," + names[1]
		key = key.strip('"')
		bat = names[2]
		runs = int(names[3])
	else:
		key = names[0]
		key = key.strip('"')
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
	"""else:
		dict_red[venue] = []
		dict_red[venue].append([runs,1,bat])"""
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
	max = -1
	for bat in dict_red[key]:
		if (max < dict_red[key][bat][2]):
			name = bat
		elif (max == dict_red[key][2]):
			if (dict_red[key][name][0] < dict_red[key][bat][0]):
				name = bat
	print (key+","+name+"\n")
