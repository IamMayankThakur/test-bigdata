import fileinput


for line in fileinput.input():
	line = line.strip()
	myList = line.split(',')

	if(myList[0] == 'info' and myList[1] == 'venue'):
		try:
			venue = (myList[2] + ', ' + myList[3])
		except:
			venue = myList[2]

	elif(myList[0] == "ball"):
		keyList = venue

		if int(myList[8]) == 0:
			valList = myList[4]+','+myList[7]
			print('%s\t%s' % (keyList,valList))

		elif (int(myList[8]) !=0) and (int(myList[7]) > 0):
			valList = myList[4]+','+myList[7]
			print('%s\t%s' % (keyList,valList))
