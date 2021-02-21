import sys
import os
import heapq

from datetime import datetime

column_details={}
input_size=0
no_partitions=0
index_list=[]
skip_data={}
record_size=0


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
    #print(record_size)


def getinputsize():
    global input_size
    input_size=os.stat('input.txt').st_size

def getpartitions():
    global no_partitions
    no_partitions=int(input_size/mem_size)
    if(no_partitions==0):
        no_partitions=1

    if(no_partitions*record_size>mem_size):
        print("Memory limit exceeded")
        sys.exit()

    f=open(input_file,'rb')
    #read_bytes=0
    for i in range(1,no_partitions+1):
        f1=open('p'+str(i),'wb')
        
        cont=f.read(mem_size)
        
        f1.write(cont)
        f1.close()

    f.close()

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

def sortpartitions():
    global skip_data

    getskipdata()

    print("##running Phase-1")
    print("Number of sub-files (splits): " + str(no_partitions)+'\n')
    #print(skip_data)

    for p in range(1,no_partitions+1):
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

def generateoutput():
    global skip_data
    hq=[]

    print("##running phase-2\n")
    fo=open(output_file,'w')

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
            os.remove(part)
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

if len(sys.argv)<5:
    print("Invalid Parameters")
    sys.exit()

input_file=sys.argv[1]
output_file=sys.argv[2]
mem_size=int(sys.argv[3])*pow(10,6)
sort_order=sys.argv[4]
tosort_on=''

for i in range(5,len(sys.argv)):
    tosort_on+=sys.argv[i]+' '
tosort_on=tosort_on[:-1].split()

print("###start execution\n")
b_time=datetime.now()
print(b_time)
readmeta()
getinputsize()
getpartitions()
getindexlst()
sortpartitions()

generateoutput()
e_time=datetime.now()
print(e_time)
print("time taken: "+ str(e_time-b_time))

#print(input_size)
#print(tosort_on)
#print(index_list)
#print(no_partitions)
#print(column_details)