import fileinput


for line in fileinput.input():
    line = line.strip()
    myList = line.split(',')

    if (myList[0] == 'ball') :
            print('%s\t%s\t%s' % (myList[4], myList[6], str(int(myList[7]) + int(myList[8]))))
