#database using files

import json
import os
import time 
import sys

#class for DB
class fileDB:
    
    filePath = ""
    rootUser = ""
    perm=True
    
    # constructor for initialising path and permmisions

    def __init__(self,path="./data.json"):
        self.filePath = path
        fileSize = os.path.getsize("./data.json")
        
        if fileSize == 0:    #if file exist
            data = {}
            data["user"]=sys.argv[0]
            F = open(path,"a")
            json.dump(data,F,indent=4)
        else:
            with open(self.filePath,mode='r') as F:
                lastData = json.load(F)
                if lastData["user"]!=sys.argv[0]:  #if user is different -> dont allow operations
                    self.perm=False
                    
            
    #function to check size of data file after every create call   
    def validateDB(self):
        file_size = os.path.getsize(self.filePath)
        
        if int(file_size)>1024:
            return True
        else:
            return False
            
        
    #create object in database with key and timestamp(optional) --->(insert)     
    def create(self,key,data,timePara=None):
        if self.perm: #if uaser is permitted
            
            if len(key)>32:
                print("Key length is more than 32 char")
            elif self.validateDB():
                print("Database size exceeds limit (1GB)")
            else:
                with open(self.filePath,mode='r') as F:
                    lastData = json.load(F)
                    
                    if key in lastData.keys():
                        print("Key already exist")
                        return
                    #if is not time given
                    if timePara==None:
                        data["time"]=None
                    else:
                        data["time"]=timePara + time.time() # using timestamp for time-to-live implimentation
                    
                    lastData[key]=data
                with open(self.filePath,mode='w+') as F:
                    json.dump(lastData,F,indent=4)
        else:
            print("Permission denied for data file usage")
            
    # Read object with give key - (get)   return obj if exist otherwise return none with messege
    def read(self,key):
        if self.perm: 

            with open(self.filePath,mode='r') as F:
                lastData = json.load(F)
                if key in lastData.keys() and (lastData[key]["time"]>time.time() or (lastData[key]["time"]==None)): #if key exist and within time to live
                    return (json.dumps(lastData[key], indent = 4))
                else:
                    print("NO key found")
        else:
            print("Permission denied for data file usage")
        return None
   
    
    # Delete object if exist
    def delete(self,key):
        if self.perm: 
            with open(self.filePath,mode='r') as F:
                lastData = json.load(F)
                if key in lastData.keys() and (lastData[key]["time"]>time.time() or (lastData[key]["time"]==None)):
                    lastData.pop(key)
                    with open(self.filePath,mode='w+') as F:
                        json.dump(lastData,F,indent=4)
                else:
                    print("NO key found")
        else:
            print("Permission denied for data file usage")
            
        
#implimentation

#create DB
DB = fileDB()  #path parameter is optional

#Insert 
DB.create("data",{"name":"user"},54)  # parametrs ->   key , obj, time-to-live

#Read
p = DB.read("data")  #return obj 
print(p)

#Delete
DB.delete("data")

