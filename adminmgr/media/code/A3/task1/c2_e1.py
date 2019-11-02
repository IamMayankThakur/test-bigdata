'''class Stat:
    @staticmethod
    def addnum(x,y):
        return x+y

#static method is used so no need to include self
s=Stat()
print(s.addnum(10,20))'''

'''class person:
    def __init__(self,name,gender):
        self.name=name
        self.gender=gender

    def getName(self):
        return self.name

    def getgender(self):
        return self.gender

class male(person):
    def __init__(self,name):
        print("hello Mr. "+name)

class female(person):
    def __init__(self,name):
        print("hello miss "+name)

class factory:
    def getperson(self,name,gender):
        if gender == 'm':
            return male(name)
        else:
            return female(name)
        
if __name__ == '__main__':
    factory=factory()
    Person=factory.getperson("Kanishk","m")

'''
# ASSIGNMENT 1


class Transport:
    #int request_count = 0
    def name(self,count):
        self.name=name
        self.count=count
    def mode(self):
        if(count < 100):
            print("We will provide you with {} trucks.".format(count//2))
        else:
            print("We will provide you with {} ships.".format(count//2))   

class Trucks:
    def __init__(self, count):
        self.count = count
    def number_of_requests(self):
        return self.count


class Ships:
    def __init__(self,count):
        print("The number of trucks we will provide with you ")
    
if __name__ == "__main__":
    transport = Transport()
    result=transport.getvehicles()
    print(result)


