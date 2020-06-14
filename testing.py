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

PATH_CONTAINER = '/Users/achintan/Desktop/error.gz'

print(f'We are in container {PATH_CONTAINER}')
CONTAINER_ID = str(PATH_CONTAINER)



#unziping the stderr.gz file from s3 bucket
with gzip.open(PATH_CONTAINER, 'rb') as f:
    file_content = f.read()

data = file_content.decode('utf-8')
data_list = data.split('\n')


ERROR_LIST = []
WARN_LIST = []
SP_WARN_LIST = []
INFO_LIST = []
KEY_WORD_LIST = ['YarnClient']

temp = ""

ERROR_STR = ""
WARN_STR = ""
INFO_STR = ""
KEYWORD_STR = ""
prev_j = ""

print(data_list[1])
#     print(data_list)
for j in data_list:
    if re.match("^[0-9][0-9][/][0-9][0-9][/][0-9][0-9]",j) or re.match("^[0-9][0-9][0-9][0-9][-][0-9][0-9][-][0-9][0-9]",j):#Picks the lines from log files starting with date format
        prev_j = j
        print(prev_j)
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
        print('Yo')      
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
            print(f'Lets see the error string {ERROR_STR}')
        


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
        print(f'Lets see temp {temp}')
    
    else:
        
        prev_j += j
        # If previous was "ERROR"
        if temp == "ERROR":
            ERROR_STR = ERROR_STR  + j + " \t "
        elif temp == "WARN":
            WARN_STR = WARN_STR  + j + " \t "
        elif temp == "INFO":
            INFO_STR = INFO_STR  + j + " \t "
    
    # print(f'Lets see ErrorList {ERROR_LIST}')

    # if len(KEY_WORD_L) > 0:
    #     keywordtest=[]
    #     for KEY_WORD in KEY_WORD_L:
    #         if KEY_WORD != "":
    #             new_j = j.lower()
    #             if KEY_WORD in new_j:
    #                 #keywordtest.append(new_j)
    #                 if(new_j not in keywordtest):
    #                     if KEYWORD_STR == "":
    #                         KEYWORD_STR = KEYWORD_STR + j + " \t "
    #                     else:
    #                         KEYWORD_STR = KEYWORD_STR + j + " \t "
                
    #             elif KEYWORD_STR != "":
    #                 if(new_j not in keywordtest):
    #                     KEYWORD_STR = KEYWORD_STR + j 
    #             keywordtest.append(new_j)


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
# print(f'Lets see the ERROR logs {LOGS_DICT[CONTAINER_ID]["ERROR"]}')
# print(f'Lets see the WARN logs {LOGS_DICT[CONTAINER_ID]["WARN"]}')
print(f'Lets see the INFO logs {INFO_LIST}')

key_dict={}
key_dict[CONTAINER_ID]=KEY_WORD_LIST
JSON_KEY.append(key_dict)
print(key_dict)
if LOGS_DICT not in JSON_ARRAY:
    JSON_ARRAY.append(LOGS_DICT)

