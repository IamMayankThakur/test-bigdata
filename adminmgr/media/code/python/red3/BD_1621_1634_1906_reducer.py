#!/usr/bin/python3
import csv
from operator import itemgetter
import sys




global current_key
global cruns
current_key = ""
cruns=0
current_count=1
lis=[]
venue=""




for line in sys.stdin:
	line = line.strip()
	line_val = line.split("\t")
	key, val = line_val[0], line_val[1]
	try:
		runs = int(val)
	except ValueError:
		continue
    
	
	if current_key == key:
		current_count += 1
		cruns=cruns+runs
		
		
		
		
	else:
		if (current_key != ""):
			
			if(current_count>=10):
        		#print '%s' % (current_key+','+str(current_count)+','+str(ball_count))
				p=current_key
				t=dict()
				a,b=p.split(";")
				srate=(cruns*100)/current_count
				t["bat"]=a
				t["venue"]=b
				t["deli"]=current_count
				t["srate"]=int(srate)*-1
				t['runs']=int(cruns)
				lis.append(t)
			
		current_count = 1
		cruns=runs
		current_key = key
    
if current_key == key:
	if(current_count>=10):
    		#print '%s' % (current_key+','+str(current_count)+','+str(ball_count))
		p=current_key
		a,b=p.split(";")
		srate=(cruns*100)/current_count
		t=dict()	
		t["bat"]=a
		t["venue"]=b
		t["deli"]=current_count
		t["srate"]=int(srate)*-1
		t['runs']=int(cruns)
		lis.append(t)
lis=sorted(lis,key=itemgetter('venue','srate','runs'))
for a in lis:
	if(venue==a['venue']):
		y=0
	
		
	else:
		print('%s,%s'%(a['venue'],a['bat']))
	

	venue=a['venue']



	#venue=a['venue']

	#print(lis)

