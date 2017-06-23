#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: raosiwei
# date: 2016-01-19

import sys,logging,ConfigParser,datetime,time
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
from basicdata import dataCrawl,dataImport 

class iData:

    def __init__(self, confPath, logPath, dataPath):
        self.conf_path = confPath
        self.log_path = logPath
        self.data_path = dataPath
        config = ConfigParser.ConfigParser()
        config.read(self.conf_path + "/istock.conf")

        self.batch_api = config.get("url", "batchAPI")
        
        fin = open(self.log_path + "/deal.log")
        sline = fin.readline().strip()
        fin.close()
        if sline == None or sline == '':
            self.init = True
            self.begin_day = config.get("history", "beginDay")
            self.end_day = config.get("history", "endDay")
        else:
            self.init = False
            lastDate = datetime.datetime.strptime(sline, "%Y-%m-%d")
            self.begin_day = (lastDate + datetime.timedelta(days=1)).strftime("%Y%m%d")
            self.end_day = time.strftime("%Y%m%d", time.localtime())


    def dataProcessing(self):
        if self.begin_day <= self.end_day: 
            dc = dataCrawl.dataCrawl(self.init, self.batch_api, self.conf_path, self.log_path, self.data_path)
            dc.setBeginEndDay(self.begin_day, self.end_day)
            dc.crawlingData()

            dimp = dataImport.dataImport(self.conf_path, self.log_path, self.data_path, self.end_day)
            dimp.importData(self.init)



if __name__ == "__main__":
    idata = iData("../conf", "../log", "../data")
    idata.dataProcessing()
 
