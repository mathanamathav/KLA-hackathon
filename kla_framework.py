import yaml
import datetime
import threading
from yaml.loader import SafeLoader
class Kla_framework:
    def __init__(self):
        self.output = []
    def list_outputfile(self,data):

        textfile = open("Milestone1B_Log.txt", "w")
        for element in data:
            textfile.write(element + "\n")
        textfile.close()

    def concurrent_task(self,name,dic):
        first = list(data)
        # print(dic)
        for val in dic:
            if dic[val]["Type"] == "Task":
                entry = dic[val]
                temp = str(datetime.datetime.now())+";"+name+first[0]+"."+str(val)+" Entry"
                self.output.append(temp)
                parameters = "("
                for index,par in enumerate(entry["Inputs"]):
                    if index == len(entry["Inputs"])-1:
                        parameters += str(entry["Inputs"][par])+")"    
                        break
                    parameters += str(entry["Inputs"][par])+","
                    
                temp = str(datetime.datetime.now())+";"+name+first[0]+"."+str(val)+" Executing "+str(entry["Function"])+" "+parameters
                self.output.append(temp)

                temp = str(datetime.datetime.now())+";"+name+first[0]+"."+str(val)+" Exit"
                self.output.append(temp)


    def logpraser(self,name,data):
        # print(data)
        # print(" ")
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
                    parameters = "("
                    for index,par in enumerate(entry["Inputs"]):
                        if index == len(entry["Inputs"])-1:
                            parameters += str(entry["Inputs"][par])+")"    
                            break
                        parameters += str(entry["Inputs"][par])+","
                        
                    temp = str(datetime.datetime.now())+";"+name+first[0]+"."+str(val)+" Executing "+str(entry["Function"])+" "+parameters
                    self.output.append(temp)

                    temp = str(datetime.datetime.now())+";"+name+first[0]+"."+str(val)+" Exit"
                    self.output.append(temp)

                elif dic[val]["Type"] == "Flow":
                    self.logpraser(str(first[0])+".",{ val : dic[val]})

        elif data[first[0]]["Execution"] == "Concurrent":
            temp = data[first[0]]["Activities"]
            threads = []
            length = len(data[first[0]]["Activities"])
            for val in data[first[0]]["Activities"]:
        
                t = threading.Thread(target=self.concurrent_task, args=[str(first[0])+".",{val : data[first[0]]["Activities"][val]}])
                t.start()
                threads.append(t)
            # for _ in range(length):
            #     t = threading.Thread(target=self.concurrent_task, args=[str(first[0])+".",{first[0] :data[first[0]]}])
            #     t.start()
            #     threads.append(t)

            for thread in threads:
                thread.join()
            return 
        val = str(datetime.datetime.now())+";"+first[0]+" Exit"
        self.output.append(val)
        self.list_outputfile(self.output)

if __name__ == "__main__":
    # with open('Examples\Milestone1\Milestone1_Example.yaml') as f:
    #     data = yaml.load(f, Loader=SafeLoader)
    
    # obj = Kla_framework()
    # obj.logpraser("",data)
    
    with open('Milestone1\Milestone1A.yaml') as f:
        data = yaml.load(f, Loader=SafeLoader)
    obj = Kla_framework()
    obj.logpraser("",data)

    # with open('Milestone1\Milestone1B.yaml') as f:
    #     data = yaml.load(f, Loader=SafeLoader)
    # obj = Kla_framework()
    # obj.logpraser("",data)
     