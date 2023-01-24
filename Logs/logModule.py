from os import path
from datetime import datetime
from typing import Dict
from types import FunctionType
import os

def loadConfig() -> Dict:
    with open('logConfig','r') as log:
        configs = log.read()
        configs = configs.replace(' ', '')
        configs = configs.split('\n')
        dictConfigs = {}
        for i in configs:
            listConfigs = i.split('=')
            if len(listConfigs) > 1:
                key = listConfigs[0]
                if key == 'archives' or key == 'levels':
                    value = listConfigs[1].split(',')
                else:
                    value = listConfigs[1]
                dictConfigAppend = {key:value}
                dictConfigs.update(dictConfigAppend)

    return dictConfigs

def logConfig():
    dictConfigs = loadConfig()
    print(dictConfigs)
    dictLogging = {}
    try:
        pathLogging = dictConfigs['path']
        archives = dictConfigs['archives']
        pathLogging = {'path':pathLogging}
        archives = {'archives':archives}
        dictLogging.update(pathLogging)
        dictLogging.update(archives)
    except KeyError as config:
        print(f"{config} not found in logConfig archive")
        exit()
    defaultValues = {'levels':['INFO','WARN','TRACE','ERROR','FATAL'],'length_message':9999,'date_format':None}
    for configs in list(defaultValues.keys()):
        try:
            vars()[configs] = dictConfigs[configs]
        except:
            vars()[configs] = defaultValues[configs]
        vars()[configs] = {configs:vars()[configs]}
        dictLogging.update(vars()[configs])

    return dictLogging


def dateFormat(configsLogging:Dict) -> str:
    date_format = configsLogging['date_format']
    if date_format == None:
        date = datetime.now()
    else:
        now = datetime.now()
        date = str(now.strftime(date_format))
    return date


def createDirectory(func:FunctionType) -> FunctionType:
    dictConfigs = logConfig(loadConfig())
    pathLogging = dictConfigs['path']
    vali = path.exists(pathLogging+'\\Logs')
    if vali != True:
        os.mkdir(pathLogging+'\\Logs')
    else:
        pass

    def createArchiveFunction(dictConfigs):
        func(dictConfigs)
    return createArchiveFunction

@createDirectory
def createArchives(dictConfigs:Dict):
    pathLogging = dictConfigs['path']
    archives = dictConfigs['archives']
    for archive in archives:
        validation = path.exists(pathLogging+'\\Logs\\'+archive+'.txt')
        if validation != True:
            open(pathLogging+'\\Logs\\'+archive+'.txt','w')
        else:
            pass



def writeLog(method:str,log:Dict):

    fullPath= r'C:\Users\Pedro\Desktop\WorkSpace\Projetos\pipelineSseraTweets\Logs'
    file = path.exists(fullPath+method+'.txt')
    if file != True:
        file = open(fullPath + method + '.txt', 'w')
        file.write(f"----- Twitter method | {method.split('L')[0].capitalize()} ----- \n")
    else:
        file = open(fullPath+method+'.txt','a')
    logMsg = str(datetime.now())+f" | {log['event']} |"+f" {log['level']} |"+f" {log['msg']} \n"
    file.write(logMsg)

