import os
from os import path
from datetime import datetime
from typing import Dict
from types import FunctionType


def createArch(func:FunctionType) -> FunctionType:

    def checkArchs(method:str,log:Dict):
        methods = ['updateLogs','insertLogs','streamingLogs']

        for value in methods:
            arch = path.exists(r'registers/'+value)
            if arch == False:
                open(r'registers/'+value,'w').write(f"----- Twitter method | {value.split('L')[0].capitalize()} ----- \n")

        func(method,log)

    return checkArchs

@createArch
def writeLog(method:str,log:Dict) -> int:
    file = open(r'registers/'+method,'a')
    logMsg = str(datetime.now())+f" | {log['event']} |"+f" {log['level']} |"+f" {log['msg']} \n"
    return file.write(logMsg)


#logteste = {'event':'INSERT', 'level':'critical','msg':'deu ruim patrao'}
#writeLog('insertLogs',logteste)
#createArch()
