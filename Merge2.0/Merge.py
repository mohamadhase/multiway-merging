import glob
import csv
import pandas as pd
from threading import Thread
class Merge:
    def __init__(self,path,blocksize): 
        self.path = path
        self.files = glob.glob(f"{path}*") # get all files in the path
        self.pointers = [] # list of pointers to files 
        self.open_files()
        self.buffers = [[] for _ in range(len(self.files))] # list of buffers for each file
        self.number_of_fills = blocksize # number of lines to fill the buffer
        self.read_threads = [Thread(target=self.fill_buffer, args=(index,))
                             for index in range(len(self.files))] # make threads to fill the buffers 
        self.merge_thread = None # thread to merge the buffers
        for thread in self.read_threads:
            thread.start() # start the threads
        for thread in self.read_threads:
            thread.join()   # wait for the threads to finish
        self.res = {} # dictionary of the result
        while(any(self.buffers)): # while there is any buffer not empty
            self.merge_thread = (Thread(target=self.merge)) # make a thread to merge the buffers
            self.merge_thread.start() # start the thread
            self.merge_thread.join() # wait for the thread to finish
        df = pd.DataFrame(self.res) # convert the dictionary to dataframe
        df.to_csv("output/output2.csv") # save the dataframe to csv file 


    def open_files(self):
        """
        this function open all files in the path and add them to pointers list 
        to keep the reading position of each file saved 
        """
        for file in self.files:
            temp = open(file,'r')
            next(temp) # skip the headers
            self.pointers.append(temp)
    def fill_buffer(self,index):
        """ this function fill the buffer of the file given by index
        Args:
            index (int): index of the buffer to be filled 
        """
        nedded_items = abs(len(self.buffers[index]) - self.number_of_fills) # number of items to be filled
        for _ in range(nedded_items):
            if line := self.pointers[index].readline(): # read a line from the file # if the line is not EOF
                line =line.strip().split(",")
                self.buffers[index].append(line)
            else: # if the line is EOF
                break
    
    def merge(self):
        """
        this function takes the first term of each buffer and get the smallest term 
        then it add the smallest term to the result dictionary and remove it from the buffers
        and fill the buffer of the file that contains the smallest term
        """
        for thread in self.read_threads:
            thread.join() # wait for the threads to finish reading # to avoid unexpected behavior 
        self.read_threads = [] # clear the threads list
        first_term = [buffer[0][0] for buffer in self.buffers if buffer] # get the first term of each buffer
        smallest_term = min(first_term) # get the smallest term
        match_buffers = [index for index,item in enumerate(self.buffers) 
                         if len(item)>0 and item[0][0]==smallest_term]# get the index of the buffers that contains the smallest term
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
        
        #refill the buffers if any one is empty using threads
        for index,buffer in enumerate(self.buffers):
            if  not buffer:
                self.read_threads.append(Thread(target=self.fill_buffer, args=(index,)))
                self.read_threads[-1].start()

