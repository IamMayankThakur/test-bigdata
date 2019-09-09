#!/usr/bin/python3
"""reducer.py"""

from operator import itemgetter
import sys
answer_dict={}
current_batsman = None
current_count = 0
current_out=0
word = None
max_wicket=0
# input comes from STDIN
for line in sys.stdin:
	line = line.strip()
	batsman,wtype,count = line.split('\t')
	try:
		count = int(count)
	except ValueError:
		continue
	if current_batsman == batsman:
		current_count += count
		if(wtype!='run out' and wtype!='retired hurt' and wtype!="\"\""):
			current_out+=1
	else:
		if(current_batsman and current_count>5):
			bat,bowler=current_batsman.split('*')
			answer_dict[(bat,bowler)]=[current_out,current_count]
		if(wtype!='runout' and wtype!='retired hurt' and wtype!=""):
			current_out=1
		current_count=count
		current_batsman=batsman
# do not forget to output the last word if needed!
if(current_batsman and current_count>5):
	bat,bowler=current_batsman.split('*')
	answer_dict[(bat,bowler)]=[current_out,current_count]
sorted_answer=sorted(answer_dict.items(),key=lambda x:(-x[1][0],x[1][1],x[0][0]))
for i in sorted_answer:
	print(i[0][0],i[0][1],i[1][0],i[1][1],sep=",")
