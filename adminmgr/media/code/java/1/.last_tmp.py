from Booking1 import *
import random
wait(2)
print("\t\t ---WELCOME TO INDIAN RAILWAYS---")
done=True
wait(1.5)
j=0
def sc(sno):
        lower=[1,4,9,12,17,20,25,28,33,36,41,44,49,52,57,60,65,68]
        middle=[2,5,10,13,18,21,26,29,34,37,42,45,50,53,58,61,66,69]
        upper=[3,6,11,14,19,22,27,30,35,38,43,46,51,54,59,62,67,70]
        side_lower=[7,15,23,31,39,47,55,63,71]
        side_upper=[8,16,24,32,40,48,56,64,72]
        if sno in lower:
            print("Seat No.",sno,"is Lower Berth\n")
        elif sno in middle:
            print("Seat No.",sno,"is Middle Berth\n")
        elif sno in upper:
            print("Seat No.",sno,"is Upper Berth\n")
        elif sno in side_lower:
            print("Seat No.",sno,"is Side Lower Berth\n")
        elif sno in side_upper:
            print("Seat No.",sno,"is Side Upper Berth\n")
        else:
            print('Seat with the given seat number does not exist\n')
        
while done:
    n=(input("\n1.Resevation\n2.PNR Status\n3.Runnning Status\n4.Seat calender\n5.Exit\n"))
    while not n.isdigit():
        n=(input("\n1.Resevation\n2.PNR Status\n3.Runnning Status\n4.Seat calender\n5.Exit\n"))
    n=int(n)
    if n==1:
        booking()
    elif n==2:
        cp=(input('Enter your pnr number :'))
        while not cp.isdigit():
                 cp=(input('Re-enter your pnr number :'))
        cp=int(cp)
        wait(3)
        if cp in PNR:
                print('Your pnr is ',cp,' and your tickets are booked.')
                print()
                print("Train Type :",data[cp][0])
                print("From :",data[cp][1])
                print("To :",data[cp][2])
                for i in range(0,data[cp][-1]):
                        print("Name of the passenger no ",i+1,":",data[cp][3][i][0])
                        print("Age :",data[cp][3][i][1])
                        print("Gender :",data[cp][3][i][4])
                        print("Coach : S",data[cp][3][i][2])
                        print("Seat :",data[cp][3][i][3])
                        print()
        else:
            print('There are no tickets booked under this pnr.')
        wait(3)
    elif n==3:
        wait(0.5)
        t=(input('\nEnter the train type: \n1.Passenger\n2.Express\n3.Superfast\n4.Shatabdi\n'))
        while not t.isdigit() or int(t)<=0 or int(t)>=5:
                t=(input('\nRe-enter the train type: \n1.Passenger\n2.Express\n3.Superfast\n4.Shatabdi\n'))
        t=int(t)
        wait(0.5)
        print("\nLoading....")
        wait(3)
        td=random.randrange(0,60,5-t)
        print('Your train',trains[t-1],'is late by',td,'minutes.')
        wait(3)
    elif n==4:
        sno=(input('Please enter the seat number :'))
        while not sno.isdigit():
                sno=(input('Please Re-enter the seat number :'))
        sno=int(sno)
        sc(sno)
    elif n==5:
        print("THANK YOU")
        done=False
    else:
        print('Please re-enter a valid choice :')