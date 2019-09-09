import sys

file_contents = sys.stdin
output_dict = dict()
for line in file_contents:
	LINE = line.strip()
	LIST = LINE.split(",")
	bowler_batsman = LIST[0] + "_" + LIST[1]
	LIST[2] = int(LIST[2])
	if bowler_batsman in output_dict.keys():
		runs_deleveries = output_dict[bowler_batsman]
		runs_deleveries[0] += LIST[2]
		runs_deleveries[1] += 1

	else:
		output_dict[bowler_batsman] = [LIST[2],1]
o = list()
for key in output_dict.keys():
	if output_dict[key][1] <=5:
		continue
	bowler_batsman = key.split("_")
	runs_deleveries = output_dict[key]	
	o.append(bowler_batsman + runs_deleveries)
o.sort(key = lambda x: (-x[2],x[3],x[0],x[1]) )
for l in o:
	print(l[0],l[1],l[2],l[3],sep=",")
	 
