import yaml
import time
import datetime
import threading
import pandas as pd
from yaml.loader import SafeLoader
class Kla_framework:
    def __init__(self):
        self.output = []
        self.datastructure = {}

    def list_outputfile(self,data):
        textfile = open("Milestone2B.txt", "w")
        for element in data:
            textfile.write(element + "\n")
        textfile.close()

    def timefunction(self,t):
        time.sleep(t)

    def dataload(self,datafile,name):
        basepath = "Milestone2\\"
        df = pd.read_csv(basepath+datafile)
        self.datastructure[name+".NoOfDefect"] = len(df)

    def concurrent_task(self,lock,name,dic):
        lock.acquire()

        first = list(data)

        for val in dic:
            if dic[val]["Type"] == "Task":
                entry = dic[val]
                temp = str(datetime.datetime.now())+";"+first[0]+"."+name+str(val)+" Entry"
                self.output.append(temp)

                if dic[val]["Function"] == "DataLoad":
                    print(entry)
                    self.dataload(entry["Inputs"]["Filename"],name+first[0]+"."+str(val))

                if entry.get("Condition",0) != 0:
                    temp = entry["Condition"]
                    if "$" in temp:
                        temp = temp.split(" ")
                        q = temp[0]
                        q = q[2:-2]
                        if self.datastructure.get(q,0) != 0:
                            if temp[1] == ">": 
                                if self.datastructure[q] > int(temp[2]):

                                    temp = str(datetime.datetime.now())+";"+first[0]+"."+name+str(val)+" Skipped"
                                    self.output.append(temp)
                                    temp = str(datetime.datetime.now())+";"+first[0]+"."+name+str(val)+" Exit"
                                    self.output.append(temp)
                                    continue

                            elif temp[1] == "<": 
                                if self.datastructure[q] < int(temp[2]):
                                    
                                    temp = str(datetime.datetime.now())+";"+first[0]+"."+name+str(val)+" Skipped"
                                    self.output(temp)
                                    temp = str(datetime.datetime.now())+";"+first[0]+"."+name+str(val)+" Exit"
                                    self.output.append(temp)
                                    continue
                            
                parameters = "("
                for index,par in enumerate(entry["Inputs"]):
                    
                    if par == "ExecutionTime":
                        self.timefunction(int(entry["Inputs"]["ExecutionTime"]))

                    if index == len(entry["Inputs"])-1:
                        parameters += str(entry["Inputs"][par])+")"    
                        break
                    
                    if "$" in str(entry["Inputs"][par]):
                            q = str(entry["Inputs"][par])
                            q = q[2:-2]
                            for key in self.datastructure:
                                if str(q) in key:
                                    q = self.datastructure[key]
                            parameters += str(q)+","
                            continue

                    parameters += str(entry["Inputs"][par])+","
                    
                temp = str(datetime.datetime.now())+";"+first[0]+"."+name+str(val)+" Executing "+str(entry["Function"])+" "+parameters
                self.output.append(temp)

                temp = str(datetime.datetime.now())+";"+first[0]+"."+name+str(val)+" Exit"
                self.output.append(temp)
            
            lock.release()
            if dic[val]["Type"] == "Flow":
                name = first[0]+"."+name
                self.logpraser(name,dic)
            


    def logpraser(self,name,data):
        dataloadflag = False
        first = list(data)
        val = str(datetime.datetime.now())+";"+name+first[0]+" Entry"
        self.output.append(val)
    
        if data[first[0]]["Execution"] == "Sequential":
            dic = data[first[0]]["Activities"]
            for val in dic:
    
                if dic[val]["Type"] == "Task":
                    
                    
                    entry = dic[val]
                    temp = str(datetime.datetime.now())+";"+name+first[0]+"."+str(val)+" Entry"
                    self.output.append(temp)
                    
                    if dic[val]["Function"] == "DataLoad":
                        self.dataload(entry["Inputs"]["Filename"],name+first[0]+"."+str(val))
                        # temp = str(datetime.datetime.now())+";"+first[0]+"."+name+str(val)+" DataLoad "+"("+entry["Inputs"]["Filename"]+")"
                        # self.output.append(temp)
                    
                    if entry.get("Condition",0) != 0:
                        print(entry)
                        temp = entry["Condition"]
                        if "$" in temp:
                            temp = temp.split(" ")
                            q = temp[0]
                            q = q[2:-2]
                        
                            if self.datastructure.get(q,0) != 0:
                                if temp[1] == ">": 
                                    if self.datastructure[q] > int(temp[2]):
                                        temp = str(datetime.datetime.now())+";"+first[0]+"."+name+str(val)+" Skipped"
                                        self.output.append(temp)
                                        temp = str(datetime.datetime.now())+";"+first[0]+"."+name+str(val)+" Exit"
                                        self.output.append(temp)
                                        continue
                                elif temp[1] == "<": 
                                    if self.datastructure[q] < int(temp[2]):
                                        temp = str(datetime.datetime.now())+";"+first[0]+"."+name+str(val)+" Skipped"
                                        self.output.append(temp)
                                        temp = str(datetime.datetime.now())+";"+first[0]+"."+name+str(val)+" Exit"
                                        self.output.append(temp)
                                        continue
                    

                    parameters = "("
                    for index,par in enumerate(entry["Inputs"]):
                        
                        if par == "ExecutionTime":
                            self.timefunction(int(entry["Inputs"]["ExecutionTime"]))
                        
                        if index == len(entry["Inputs"])-1:
                            parameters += str(entry["Inputs"][par])+")"
                            break

                        if "$" in str(entry["Inputs"][par]):
                            q = str(entry["Inputs"][par])
                            q = q[2:-2]
                            for key in self.datastructure:
                                if q in key:
                                    q = self.datastructure[key]
                            parameters += str(q)+","
                            continue

                        parameters += str(entry["Inputs"][par])+","
                    
                    temp = str(datetime.datetime.now())+";"+name+first[0]+"."+str(val)+" Executing "+str(entry["Function"])+" "+parameters
                    self.output.append(temp)

                    temp = str(datetime.datetime.now())+";"+name+first[0]+"."+str(val)+" Exit"
                    self.output.append(temp)
                
                elif dic[val]["Type"] == "Flow":
                    self.logpraser(str(first[0])+"."+name,{ val : dic[val]})
                 
                

        elif data[first[0]]["Execution"] == "Concurrent":
            temp = data[first[0]]["Activities"]
            threads = []
            
            for val in data[first[0]]["Activities"]:
                lock = threading.Lock()
                t = threading.Thread(target=self.concurrent_task, args=[lock,str(first[0])+".",{val : data[first[0]]["Activities"][val]}])
                t.start()
                threads.append(t)
                
        
            for thread in threads:
                thread.join()

        val = str(datetime.datetime.now())+";"+name+first[0]+" Exit"
        self.output.append(val)

        print(self.datastructure)
        self.list_outputfile(self.output)

if __name__ == "__main__":
    # with open('Examples\Milestone1\Milestone1_Example.yaml') as f:
    #     data = yaml.load(f, Loader=SafeLoader)
    
    # obj = Kla_framework()
    # obj.logpraser("",data)
    
    # with open('Milestone1\Milestone1B.yaml') as f:
    #     data = yaml.load(f, Loader=SafeLoader)
    # obj = Kla_framework()
    
    with open('Milestone2\Milestone2B.yaml') as f:
        data = yaml.load(f, Loader=SafeLoader)
    obj = Kla_framework()
    obj.logpraser("",data)