#!/usr/bin/python3
import sys
import operator

stkrate = {}
result = {}
res = {}


for row in sys.stdin:

	row = row.strip()
	vbr, ball = row.split(';')

	vb,run = vbr.split('$')
	venue, bat = vb.split('&')
	
	if venue not in stkrate:

		stkrate[venue] = {}
		result[venue] = {}

	if(bat not in stkrate[venue]):

		stkrate[venue][bat]=[int(run),int(ball)]

	else:

		stkrate[venue][bat][0]=stkrate[venue][bat][0]+run
		stkrate[venue][bat][1]=stkrate[venue][bat][1]+ball
	


for place in stkrate:

	for bm in stkrate[place]:

		if stkrate[place][bm][1]>=10:

			d=100/stkrate[place][bm][1]
			result[place][bm]=stkrate[place][bm][0]*d
for place in result:

	
	l=[key for key,value in result[place].items() if value == max(result[place].values())]

	if len(l)==1:

		res[place] = l[0]

	else:

		p = 0
		for r in l:

			if stkrate[place][b][0] >r:

				p = stkrate[place][r][0]
				y = r
				res[place]=y

k = sorted(res.items(), key = lambda i: i[0])
res=dict(k)

for place in res:

	print ('%s,%s'% (place,res[place]))


