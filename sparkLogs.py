from flask import *
from json import *
from flask_cors import CORS, cross_origin
from flask import Flask, request, jsonify, send_from_directory

from io import BytesIO
import gzip
import re
import json
import threading 
import time
import concurrent.futures
import os

# app = Flask(__name__)
# CORS(app)


SPECIFIC_ERROR_L=[]
SPECIFIC_WARN_L=[]
JSON_ARRAY = []
JSON_KEY = []




def info_check(INFO_STR):
    keywords = ["Caused by:"]
    for k in keywords:
        if k in INFO_STR:
            return True
    return False

def sp_error_check(JsonRes):
    
    keywords_sp_error = ["ERROR TaskSetManager",
    "Job aborted due to stage failure",
    "ERROR Executor",
    "ERROR ApplicationMaster",
    "Broken pipejava.io.IOException",
    "Executor self-exiting due to : Driver",
    "ERROR TransportResponseHandler",
    "ERROR TransportClient",
    "ERROR RetryingBlockFetcher: Exception while beginning fetch",
    "ERROR Utils",
    "ERROR SparkHadoopWriter: Aborting job",
    "OneForOneBlockFetcher: Failed while starting block fetchesjava.io.IOException",
    "ERROR RpcOutboxMessage",
    "ERROR SparkContext: Error initializing SparkContext","java.nio.file.AccessDeniedException"
    ]
    Jsonlength=len(JsonRes)
    for SError in keywords_sp_error:
        SPECIFIC_ERROR = {}
        SPECIFIC_ERROR[SError]={}
        for i in range(Jsonlength):
            temp=JsonRes[i]
            ContaineridL=list(temp.keys())
            ContainerID=ContaineridL[0]
            value=temp[ContainerID]
            ErrL=value['ERROR']
            for err in ErrL:
                if SError in err:
                    if ContainerID in SPECIFIC_ERROR[SError]:
                        SPECIFIC_ERROR[SError][ContainerID].append(err)
                    else:
                        SPECIFIC_ERROR[SError][ContainerID]=[]
                        SPECIFIC_ERROR[SError][ContainerID].append(err)
 
        if SPECIFIC_ERROR not in SPECIFIC_ERROR_L:
            SPECIFIC_ERROR_L.append(SPECIFIC_ERROR)
    return SPECIFIC_ERROR_L
#-----------------------------------------------#


#--------FUNCTION TO CHECK FOR SPECIFIC KEYWORDS IN WARN---------#
def sp_warn_check(JsonRes):
    
    keywords_sp_warn = ["exception org.apache.spark.TaskKilledException",
    "failed due to exception java.lang.IllegalStateException",
    "could not be removed as it was not found on disk or in memory",
    "OneWayOutboxMessage: Failed to send one-way RPC",
    "WARN NettyRpcEnv: Ignored failure: java.io.IOException: Failed to send RPC",
    "Failed to fetch remote block",
    "Exception in connection",
    "WARN YarnAllocator: Container killed by YARN for exceeding memory limits",
    "failed due to exception org.apache.spark.SparkException: No port number",
    "NioEventLoop: Selector.select() returned prematurely",
    "exception org.apache.spark.SparkException: Exception thrown in awaitResult",
    "WARN YarnAllocator: Container from a bad node",
    "Issue communicating with driver in heartbeaterorg.apache.spark.SparkException",
    "WARN SparkConf: spark.master yarn-cluster is deprecated in Spark 2.0+",
    "Unable to eagerly init filesystem s3://does/not/exist",
    "SparkConf: The configuration key 'spark.yarn.executor.memoryOverhead' has been deprecated",
    "WARN Executor: Issue communicating with driver in heartbeater","WARN TaskSetManager"
    ]
    
    Jsonlength=len(JsonRes)
    for SWarn in keywords_sp_warn:
        SPECIFIC_WARN = {}
        SPECIFIC_WARN[SWarn]={}
        for i in range(Jsonlength):
            temp=JsonRes[i]
            ContaineridL=list(temp.keys())
            ContainerID=ContaineridL[0]
            value=temp[ContainerID]
            WarnL=value['WARN']
            for war in WarnL:
                if SWarn in war:
                    if ContainerID in SPECIFIC_WARN[SWarn]:
                        SPECIFIC_WARN[SWarn][ContainerID].append(war)
                    else:
                        SPECIFIC_WARN[SWarn][ContainerID]=[]
                        SPECIFIC_WARN[SWarn][ContainerID].append(war)
            
        if SPECIFIC_WARN not in SPECIFIC_WARN_L:
            SPECIFIC_WARN_L.append(SPECIFIC_WARN)
    return SPECIFIC_WARN_L
#-----------------------------------------------------#








#-----FUNCTION TO FETCH LOGS FOR EACH CONTAINER FILE-----#
def container(p):
    global JSON_ARRAY
    global JSON_KEY
    PATH_CONTAINER = p[0] #path to each container.
    KEY_WORD_L = p[1] #List of keywords entered by user.
    
    print(f'We are in conatiner {PATH_CONTAINER}')
    CONTAINER_ID = str(PATH_CONTAINER)


    try:
        #unziping the stderr.gz file from s3 bucket
        with gzip.open(PATH_CONTAINER, 'rb') as f:
            file_content = f.read()

        data = file_content.decode('utf-8')
        data_list = data.split('\n')
        

        ERROR_LIST = []
        WARN_LIST = []
        SP_WARN_LIST = []
        INFO_LIST = []
        KEY_WORD_LIST = []

        temp = ""
        
        ERROR_STR = ""
        WARN_STR = ""
        INFO_STR = ""
        KEYWORD_STR = ""
        prev_j = ""
        
        

        for j in data_list:
       
            if re.match("^[0-9][0-9][/][0-9][0-9][/][0-9][0-9]",j) or re.match("^[0-9][0-9][0-9][0-9][-][0-9][0-9][-][0-9][0-9]",j):#Picks the lines from log files starting with date format
                prev_j = j
                
                if KEYWORD_STR != "":
                    if not KEY_WORD_LIST:
                        check_str = KEYWORD_STR[0:1]
                        if check_str == '\t':
                            
                            KEYWORD_STR = KEYWORD_STR[1:]
                            KEY_WORD_LIST.append(KEYWORD_STR)
                            KEYWORD_STR = ""
                        else:
                            KEY_WORD_LIST.append(KEYWORD_STR)
                            KEYWORD_STR = ""
                    else:
                        KEY_WORD_LIST.append(KEYWORD_STR)
                        KEYWORD_STR = ""
                        
                test1 = j.split()

                if test1[2] == "ERROR": #If log file contains an error line
                    if temp == "ERROR":
                        ERROR_LIST.append(ERROR_STR)
                        ERROR_STR = ""

                    if INFO_STR != "":
                        if info_check(INFO_STR):
                            INFO_LIST.append(INFO_STR)
                        INFO_STR = ""
                        
                    if WARN_STR != "":
                        WARN_LIST.append(WARN_STR)
                        WARN_STR = ""
                    ERROR_STR = ERROR_STR  + j + " \t "
                    


                elif test1[2] == "WARN": #If log file contains a warn line
                    if temp == "WARN":
                        WARN_LIST.append(WARN_STR)
                        WARN_STR = ""

                    if ERROR_STR != "":
                        ERROR_LIST.append(ERROR_STR)
                        ERROR_STR = ""
                        
                    if INFO_STR != "":
                        if info_check(INFO_STR):
                            INFO_LIST.append(INFO_STR)
                        INFO_STR = ""
                    WARN_STR = WARN_STR  + j + " \t "
                    
                    
                elif test1[2] == "INFO": #If log file contains a info line
                    if temp == "INFO":
                        if info_check(INFO_STR):
                            INFO_LIST.append(INFO_STR)
                        INFO_STR = ""

                    if ERROR_STR != "":
                        ERROR_LIST.append(ERROR_STR)
                        ERROR_STR = ""
                        
                    if WARN_STR != "":
                        WARN_LIST.append(WARN_STR)
                        WARN_STR = ""
                    INFO_STR = INFO_STR  + j + " \t "
                


                temp = test1[2]
            
            else:
                prev_j += j
                # If previous was "ERROR"
                if temp == "ERROR":
                    ERROR_STR = ERROR_STR  + j + " \t "
                elif temp == "WARN":
                    WARN_STR = WARN_STR  + j + " \t "
                elif temp == "INFO":
                    INFO_STR = INFO_STR  + j + " \t "
            

            
            if len(KEY_WORD_L) > 0:
                keywordtest=[]
                for KEY_WORD in KEY_WORD_L:
                    if KEY_WORD != "":
                        new_j = j.lower()
                        if KEY_WORD in new_j:
                            #keywordtest.append(new_j)
                            if(new_j not in keywordtest):
                                if KEYWORD_STR == "":
                                    KEYWORD_STR = KEYWORD_STR + j + " \t "
                                else:
                                    KEYWORD_STR = KEYWORD_STR + j + " \t "
                        
                        elif KEYWORD_STR != "":
                            if(new_j not in keywordtest):
                                KEYWORD_STR = KEYWORD_STR + j 
                        keywordtest.append(new_j)
        

        if KEYWORD_STR != "":
            if not KEY_WORD_LIST:
                check_str = KEYWORD_STR[0:1]
                if check_str == '\t':
                    
                    KEYWORD_STR = KEYWORD_STR[1:]
                    KEY_WORD_LIST.append(KEYWORD_STR)
                    KEYWORD_STR = ""
                else:
                    KEY_WORD_LIST.append(KEYWORD_STR)
                    KEYWORD_STR = ""
            else:
                KEY_WORD_LIST.append(KEYWORD_STR)
                KEYWORD_STR = ""
                
                
        if temp == "ERROR" and ERROR_STR != "":
            ERROR_LIST.append(ERROR_STR)

        elif temp == "WARN" and WARN_STR != "":
            WARN_LIST.append(WARN_STR)

        elif temp == "INFO" and INFO_STR != "":
            if info_check(INFO_STR):
                INFO_LIST.append(INFO_STR)

                
        # DICT FOR ERROR AND WARN
        LOGS_DICT={}
        #Appending error, warn and info for a particular container
        LOGS_DICT[CONTAINER_ID] = {}
        LOGS_DICT[CONTAINER_ID]["ERROR"] = ERROR_LIST 
        LOGS_DICT[CONTAINER_ID]["WARN"] = WARN_LIST
        LOGS_DICT[CONTAINER_ID]["INFO"] = INFO_LIST
        # print(f'Lets see the ERROR logs {LOGS_DICT["ERROR"]}')
        # print(f'Lets see the WARN logs {LOGS_DICT["WARN"]}')
        
        key_dict={}
        key_dict[CONTAINER_ID]=KEY_WORD_LIST
        JSON_KEY.append(key_dict)
        if LOGS_DICT not in JSON_ARRAY:
            JSON_ARRAY.append(LOGS_DICT)

    except Exception as e:
        print("except")
        pass


#-----------------------------------------------------------#"""

#-----FUNCTION FOR KEYWORD SEARCH------#
def container_keyword_research(p):
    global JSON_ARRAY
    global JSON_KEY

    PATH_CONTAINER = p[0] #path to each container.
    KEY_WORD_L = p[1] #List of keywords entered by user.
    

    CONTAINER_ID = str(PATH_CONTAINER)


    try:
        #unziping the stderr.gz file from s3 bucket
        with gzip.open(PATH_CONTAINER, 'rb') as f:
            file_content = f.read()

        data = file_content.decode('utf-8')
        data_list = data.split('\n')

        
        KEY_WORD_LIST = []

        temp = ""
        
    
        KEYWORD_STR = ""
        prev_j = ""

        

        for j in data_list:
            
            #
            if re.match("^[0-9][0-9][/][0-9][0-9][/][0-9][0-9]",j) or re.match("^[0-9][0-9][0-9][0-9][-][0-9][0-9][-][0-9][0-9]",j): #Picks the lines from log files starting with date format
                prev_j = j
               
                
                if KEYWORD_STR != "":
                    if not KEY_WORD_LIST:
                        check_str = KEYWORD_STR[0:1]
                        if check_str == '\t':
                            
                            KEYWORD_STR = KEYWORD_STR[1:]
                            KEY_WORD_LIST.append(KEYWORD_STR)
                            KEYWORD_STR = ""
                        else:
                            KEY_WORD_LIST.append(KEYWORD_STR)
                            KEYWORD_STR = ""
                    else:
                        KEY_WORD_LIST.append(KEYWORD_STR)
                        KEYWORD_STR = ""
                        
            else:
                prev_j += j
                
            
            
            
            
            if len(KEY_WORD_L) > 0:
                keywordtest=[]
                for KEY_WORD in KEY_WORD_L:
                    if KEY_WORD != "":
                        new_j = j.lower()
                        if KEY_WORD in new_j:
                            #keywordtest.append(new_j)
                            if(new_j not in keywordtest):
                                if KEYWORD_STR == "":
                                    KEYWORD_STR = KEYWORD_STR + j + " \t "
                                else:
                                    KEYWORD_STR = KEYWORD_STR + j + " \t "
                        
                        elif KEYWORD_STR != "":
                            if(new_j not in keywordtest):
                                KEYWORD_STR = KEYWORD_STR + j 
                        keywordtest.append(new_j)
                            
          

        if KEYWORD_STR != "":
            #---To handle a particular error---#
            if not KEY_WORD_LIST:
                check_str = KEYWORD_STR[0:1]
                if check_str == '\t':
                    
                    KEYWORD_STR = KEYWORD_STR[1:]
                   
                    KEY_WORD_LIST.append(KEYWORD_STR)
                    KEYWORD_STR = ""
                else:
                   
                    KEY_WORD_LIST.append(KEYWORD_STR)
                    KEYWORD_STR = ""
            else:
                #---To handle a particular error---#
               
                KEY_WORD_LIST.append(KEYWORD_STR)
                KEYWORD_STR = ""
                

                
        # DICT FOR ERROR AND WARN
        key_dict={}
        key_dict[CONTAINER_ID]=KEY_WORD_LIST
        JSON_KEY.append(key_dict)
    except Exception as e:
        print("except")
        pass

#---------------------------------------------------------#





def getListOfFiles(dirName):
    # create a list of file and sub directories
    # names in the given directory
    listOfFile = os.listdir(dirName)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory
        if os.path.isdir(fullPath):
            allFiles = allFiles + getListOfFiles(fullPath)
        else:
            allFiles.append(fullPath)

    return allFiles



# def MainProgram(CLUSTER_ID,APPLICATION_ID,KEY_WORD,EXCLUSIVE,LOGPATH):
def MainProgram(KEY_WORD,EXCLUSIVE,LOGPATH):
    #PARAMETERS = request.json
   
    # start = time.perf_counter() #start time
    # global BUCKET_NAME
    # global FLAG
    # global JSON_KEY
    

    if LOGPATH[-1]=='/':
        LOGPATH=LOGPATH[0:len(LOGPATH)-1]
    K_LIST = KEY_WORD.split(",") #List containing the keywords

    data = list()
    # data=getListOfFiles(LOGPATH+'/'+CLUSTER_ID+'/containers/'+APPLICATION_ID)
    data=getListOfFiles(LOGPATH)

    with concurrent.futures.ThreadPoolExecutor() as executor: #multithreading- unique thread is created per file.

        results = []
  
        for i in data:
            if re.match("^.*stderr\.gz$", str(i)): #match for stderr.gz file
               
                # LOOP THROUGH EACH CONTAINER WITH STDERR.GZ FILE
                PATH_CONTAINER = str(i)

    #-----------New thread is created for each file----------------------#
                
                p = (PATH_CONTAINER, K_LIST)
                if EXCLUSIVE == False:
                    
                    f = executor.submit(container, p) #calling container function
                else:
                    print('Jobs are running')
                    f = executor.submit(container_keyword_research, p) #calling container_keyword_research function
                   
                results.append(f)

        for r in concurrent.futures.as_completed(results):
            r.result()
    


    #--------------------------------------------------------------------#

    finish = time.perf_counter() #Finish time

    #--------------------------FOR ERROR AND WARN------------------#

    SPECIFIC_RES_E = [] #List for specific errors
    SPECIFIC_RES_W = [] #List for specific warnings
    
    FINAL_LOGS_DICT = {}
    JSON_ARRAY.sort(key=lambda k:(list(k.keys())[0]))
    FINAL_LOGS_DICT['filepath']=" "
    
    #Appending the search results to FINAL_LOGS_DICT
    if EXCLUSIVE == False:
        a=SPECIFIC_RES_E.copy()
        b=SPECIFIC_RES_W.copy()
        SPECIFIC_RES_E = sp_error_check(JSON_ARRAY)
        FINAL_LOGS_DICT['SpecificError']=SPECIFIC_RES_E
        SPECIFIC_RES_W = sp_warn_check(JSON_ARRAY)
        c=JSON_ARRAY.copy()
        FINAL_LOGS_DICT['SpecificWarn']=SPECIFIC_RES_W
        FINAL_LOGS_DICT['res'] = c
        FINAL_LOGS_DICT['flag'] = 0
        d1=JSON_KEY.copy()
        FINAL_LOGS_DICT['Keyword']=d1

        
    else:
        FINAL_LOGS_DICT['res'] = []
        FINAL_LOGS_DICT['SpecificError']=[]
        FINAL_LOGS_DICT['SpecificWarn']=[]
        FINAL_LOGS_DICT['flag'] = 0
        d1=JSON_KEY.copy()
        FINAL_LOGS_DICT['Keyword']=d1
        
    #print("Res Len ", len(str(FINAL_LOGS_DICT['res'])))
    #print("Keyword Len ",len(str(FINAL_LOGS_DICT['Keyword'])))

    FINAL_LOGS_DICT['resLen']=len(str(FINAL_LOGS_DICT['res']))

    if(len(str(FINAL_LOGS_DICT['Keyword'])) <=135000000): 
        FINAL_LOGS_DICT['KeywordLen']=len(str(FINAL_LOGS_DICT['Keyword']))

    else:
        FINAL_LOGS_DICT['Keyword']=[]
        FINAL_LOGS_DICT['flag'] = 1
        FINAL_LOGS_DICT['KeywordLen']=len(str(FINAL_LOGS_DICT['Keyword']))
        
    print('')
    print('')
    # print('FINAL_LOGS_DICT',FINAL_LOGS_DICT['Keyword'])
    #print("Size ", len(str(FINAL_LOGS_DICT)))
    # JSON_RESULT = json.dumps(FINAL_LOGS_DICT, sort_keys=True)
    # # print(f'LET SEE JSON ARRAY{JSON_ARRAY}')
    # JSON_ARRAY.clear()
    # SPECIFIC_RES_E.clear()
    # SPECIFIC_RES_W.clear()
    # JSON_KEY.clear()
    # print(f'Lets use the result of SPECIFIC_RES_E{ FINAL_LOGS_DICT["SpecificError"]} ')

    return FINAL_LOGS_DICT
    


if __name__ == '__main__' :
    result = MainProgram('',False,'/Users/achintan/Desktop/python-virtual-env/appLogs/logs1')
    for key in result['SpecificError']:
        print(key)
  
    # with open("sample.json", "w") as outfile: 
    #     json.dump(result['SpecificError'], outfile) 