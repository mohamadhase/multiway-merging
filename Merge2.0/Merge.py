import glob
import csv
import pandas as pd
from threading import Thread
class Merge:
    def __init__(self,path,blocksize):  # sourcery skip: for-index-underscore
        self.path = path
        self.files = glob.glob(f"{path}*")
        self.pointers = []
        self.open_files()
        self.buffers = [[] for i in range(len(self.files))]
        self.number_of_fills = blocksize
        self.read_threads = [Thread(target=self.fill_buffer, args=(index,)) for index in range(len(self.files))]
        self.merge_thread = None
        for thread in self.read_threads:
            thread.start()
        for thread in self.read_threads:
            thread.join()
        self.res = {}
        while(any(self.buffers)):
            self.merge_thread = (Thread(target=self.merge))
            self.merge_thread.start()
            self.merge_thread.join()
        df = pd.DataFrame(self.res)
        df.to_csv("output/output2.csv")



    def open_files(self):
        for file in self.files:
            temp = open(file,'r')
            next(temp) # skip the headers
            self.pointers.append(temp)
    def fill_buffer(self,index):
        nedded_items = abs(len(self.buffers[index]) - self.number_of_fills)
        for _ in range(nedded_items):
            if line := self.pointers[index].readline():
                line =line.strip().split(",")
                self.buffers[index].append(line)
            else:
                break
    
    def merge(self):
        for thread in self.read_threads:
            thread.join()
        self.read_threads = []
        #get the first term of each buffer
        first_term = [buffer[0][0] for buffer in self.buffers if buffer]
        #get the smallest term 
        smallest_term = min(first_term)
        #get the indexes of files have the term
        match_buffers = [index for index,item in enumerate(self.buffers) 
                         if len(item)>0 and item[0][0]==smallest_term]
        #merge the list of same term to one list and add it to res
        merged_list = []
        for index_match in match_buffers:
            temp = self.buffers[index_match][0] [2:]
            merged_list+=temp
        merged_list.insert(0,len(merged_list))
        self.res[smallest_term] = merged_list
        #remove from the buffer 
        for index in match_buffers:
            self.buffers[index] = (self.buffers[index][1:])
        
        #refill the buffers if any one is empty
        for index,buffer in enumerate(self.buffers):
            if  not buffer:
                self.read_threads.append(Thread(target=self.fill_buffer, args=(index,)))
                self.read_threads[-1].start()

