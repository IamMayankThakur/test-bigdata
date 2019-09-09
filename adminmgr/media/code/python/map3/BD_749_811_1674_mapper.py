#!/usr/bin/python3
import sys
import csv
venue = ''
for line in sys.stdin:
	#print(line)
	line = line.strip()
	#print(line)
	#if(line[1]=='venue' and line[2]=='Rajiv Gandhi International Stadium, Uppal'):
	#	venue=line[2]
	li = line.split(',')
	 
	if(li[0] == 'info' and li[1] == 'venue'):
		venue = li[2]
		if (li[2]=='"Rajiv Gandhi International Stadium' ):
			venue=venue+', Uppal"'
		if(li[2]=='"Punjab Cricket Association IS Bindra Stadium'):
			venue=venue+', Mohali"'
		if(li[2]=='"MA Chidambaram Stadium'):
			venue=venue+', Chepauk"'
		if(li[2]=='"Sardar Patel Stadium'):
			venue=venue+', Motera"'
		if(li[2]=='Punjab Cricket Association Stadium'):
			venue=venue+', Mohali"'
		if(li[2]=='Vidarbha Cricket Association Stadium'):
			venue=venue+', Jamtha"'





	elif(li[0] == "ball"):
		key_list = venue
		val_list = li[4]+','+li[7]
		if(int(li[8]) != 0):
			continue
		print('%s\t%s' % (key_list,val_list))  
