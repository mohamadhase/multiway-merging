import glob
import csv
import pandas as pd
class Merge:
    def __init__(self,path,blocksize):  
        self.path = path
        self.files = glob.glob(f"{path}*")  # get all files in the path
        self.pointers = [] # list of pointers to files
        self.open_files()
        self.buffers = [[] for _ in range(len(self.files))] # list of buffers for each file
        self.number_of_fills = blocksize  # number of lines to fill the buffer
        for index in range(len(self.files)): #fill buffers for the first time
            self.fill_buffer(index)
        self.res = {} # dictionary of results {term:[len,doc1,doc2,doc3,...]}
        while(any(self.buffers)): # while any buffer is not empty
            self.merge() 
        df = pd.DataFrame(self.res)
        df.to_csv("output/output.csv",mode='a')

    def open_files(self):
        """
        this function open all files in the path and add them to pointers list 
        to keep the reading position of each file saved 
        """
        for file in self.files:
            temp = open(file,'r')
            next(temp) # move the pointer to the second line to skip the header 
            self.pointers.append(temp)
            
    def fill_buffer(self,index):
        """ this function fill the buffer of the file given by index
        Args:
            index (int): index of the buffer to be filled 
        """
        nedded_items = abs(len(self.buffers[index]) - self.number_of_fills) # number of items to be filled 
        for _ in range(nedded_items):
            if line := self.pointers[index].readline(): # read line from the file and if its not EOF
                line =line.strip().split(",")
                self.buffers[index].append(line)
            else: # if EOF
                break
    
    def merge(self):
        """
        this function takes the first term of each buffer and get the smallest term 
        then it add the smallest term to the result dictionary and remove it from the buffers
        and fill the buffer of the file that contains the smallest term
        """
        first_term = [buffer[0][0] for buffer in self.buffers if buffer] # get the first term of each buffer
        smallest_term = min(first_term) # get the smallest term
        match_buffers = [index for index,item in enumerate(self.buffers) 
                         if len(item)>0 and item[0][0]==smallest_term] # get the index of buffers that have the smallest term
        #merge the list of same term to one list and add it to res
        merged_list = []
        for index_match in match_buffers:
            temp = self.buffers[index_match][0] [2:]
            merged_list+=temp
        merged_list.insert(0,len(merged_list))
        self.res[smallest_term] = merged_list
        for index in match_buffers:
            self.buffers[index] = (self.buffers[index][1:]) # remove the first item from the buffer
        
        #refill the buffers if any one is empty
        for index,buffer in enumerate(self.buffers):
            if  not buffer:
                self.fill_buffer(index)

