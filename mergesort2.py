import sys
import os
import math
import heapq
import threading

from datetime import datetime

column_details={}
input_size=0
no_partitions=0
index_list=[]
skip_data={}
part_each_thread=0
assigned_threads=0
phase1=False
record_size=0
c=0

class MyObject(object):
    def __init__(self, val):
        #print(val)
        self.val = val
    def __lt__(self, other):
        if sort_order=='asc':
            return self.val[0]<other.val[0]
        elif sort_order=='desc':
            return self.val[0]>other.val[0]
        #return cmp(self.val, other.val)

def readmeta():
    global column_details
    global record_size

    f=open('metadata.txt','r')
    cont=f.read().strip()
    mylst=cont.split('\n')
    for item in mylst:
        col=item.split(',')
        column_details[col[0]]=int(col[1])
        record_size+=int(col[1])
    
    record_size+=2*(len(mylst)-1)
    f.close()


def getinputsize():
    global input_size
    input_size=os.stat('input.txt').st_size

def getdetails():
    global no_partitions
    global no_threads
    global part_each_thread

    no_partitions=int(input_size/mem_size)

    if(no_partitions==0):
        no_partitions=1

    if no_partitions < no_threads:
        no_threads=no_partitions
        part_each_thread=1
    else:
        part_each_thread=math.ceil(no_partitions/no_threads)

def getpartitions():
    global assigned_threads
    assigned_threads+=1

    
    #read_bytes=0
    loop_from=(assigned_threads-1)*(part_each_thread)+1
    for i in range(loop_from,loop_from+part_each_thread):
        if(i>no_partitions):
            return

        if(no_partitions*record_size>mem_size):
            print("Memory limit exceeded")
            sys.exit()

        print("i is:",i)
        f1=open('p'+str(i),'wb')
        print('p'+str(i))
        
        cont=f.read(mem_size)
        
        f1.write(cont)
        f1.close()

    
    sortpartitions(loop_from)

def getindexlst():
    global index_list
    col_list=list(column_details.keys())

    for col in tosort_on:
        index_list.append(col_list.index(col))

def getskipdata():
    global skip_data
    for col in tosort_on:
            skip_data[col]=0
            for i in list(column_details.keys()):
                if i!=col:
                    skip_data[col]+=(column_details[i])+2
                else:
                    break

def sort_lists(file_lst,str_lst):
    zipped_pairs = zip(str_lst, file_lst)
    
    if sort_order=='asc':
        z = [x for _, x in sorted(zipped_pairs)]
    elif sort_order=='desc':
        z = [x for _, x in sorted(zipped_pairs,reverse=True)]
     
    return z

def sortpartitions(loop_from):
    global skip_data
    global part_each_thread
    global assigned_threads
    global phase1
    global c

    getskipdata()

    #print("##running Phase-1")
    #print("Number of sub-files (splits): " + str(no_partitions)+'\n')
    

    for p in range(loop_from,loop_from+part_each_thread):
        if(p>no_partitions):
            return
        f=open('p'+str(p),'r')
        
        file_lst=f.readlines()
        temp_str=''

        f.close()
        #print(file_lst)
        #print("after")

        str_lst=[]
        for row in file_lst:
            temp_str=''
            for i in list(skip_data.keys()):
                read_till=skip_data[i]+column_details[i]
                temp_str+=row[skip_data[i]:read_till]
            str_lst.append(temp_str)

        print("sorting #"+str(p)+" sublist")
        file_lst=sort_lists(file_lst,str_lst)
        #print(file_lst)

        print("Writing to disk #"+str(p)+'\n')
        f=open('p'+str(p),'w')
        for row in file_lst:
            f.write(row)
        f.close()

        c+=1
        if c==no_threads:
            print("early")
            phase1=True

def generateoutput():
    global skip_data
    hq=[]

    print("##running phase-2\n")
    fo=open(output_file,'a')

    part_content={}
    part_ptr={}
    for p in range(1,no_partitions+1):
        fi=open('p'+str(p),'r')
        part_ptr['p'+str(p)]=fi

        cont=fi.readline()
        temp_lst=[]

        temp_str=''
        for i in list(skip_data.keys()):
            read_till=skip_data[i]+column_details[i]
            temp_str+=cont[skip_data[i]:read_till]
        temp_lst.append(temp_str)
        temp_lst.append('p'+str(p))

        part_content['p'+str(p)]=cont
        heapq.heappush(hq, MyObject(temp_lst))
        
    print("Sorting...\n")
    print("Writing to disk\n")

    while len(hq)!=0:
        obj = heapq.heappop(hq)
        #print(type(obj.val))
        part=obj.val[1]
        cont=str(part_content[part])
        
        fo.write(cont)

        re_cont=part_ptr[part].readline() 
        if(len(re_cont)==0):
            part_content.pop(part)
            #os.remove(part)
            part_ptr[part].close()
            continue

        temp_str=''
        temp_lst=[]
        for i in list(skip_data.keys()):
            read_till=skip_data[i]+column_details[i]
            temp_str+=re_cont[skip_data[i]:read_till]
        temp_lst.append(temp_str)
        temp_lst.append(part)

        part_content[part]=re_cont
        heapq.heappush(hq, MyObject(temp_lst))        

    fo.close()
    print("###completed execution")

if len(sys.argv)<6:
    print("Invalid Parameters")
    sys.exit()

input_file=sys.argv[1]
output_file=sys.argv[2]
mem_size=int(sys.argv[3])*pow(10,6)
no_threads=int(sys.argv[4])
sort_order=sys.argv[5]
tosort_on=''

fii=open(output_file,'w')
fii.close()
for i in range(6,len(sys.argv)):
    tosort_on+=sys.argv[i]+' '
tosort_on=tosort_on[:-1].split()

print("###start execution\n")
b_time=datetime.now()
print(b_time)
readmeta()
getinputsize()

getdetails()
getindexlst()
print(no_threads)
f=open(input_file,'rb')
for i in range(no_threads):
    t1=threading.Thread(target=getpartitions,args=())
        
    t1.start()
    t1.join()


    
#getpartitions()

#sortpartitions()

#while True:
#    if phase1==True:
f.close()
generateoutput()
for i in range(no_partitions):
    os.remove('p'+str(i+1))
e_time=datetime.now()
print(e_time)
print("time taken: "+ str(e_time-b_time))
#        break

#print(input_size)
#print(tosort_on)
#print(index_list)
#print(no_partitions)
#print(column_details)