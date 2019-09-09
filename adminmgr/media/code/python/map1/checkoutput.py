f=open("open3.txt","r")
f1=open("output2_3.txt","r")
l=f.readlines()
l1=f1.readlines()
c=0
for x,y in zip(l,l1):
    if(x.strip() != y.strip()):
        print(c,"Not equal")
        print(x,y)
        break
    else:
        c=c+1
if(c==len(l)):
    print(c,"Equal")