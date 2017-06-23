#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: raosiwei
# date: 2016-01-19

import sys,urllib,time,logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
import socket
socket.setdefaulttimeout(10.0)
import pymongo


class dataCrawl:

    def __init__(self, init, batchAPI, confPath, logPath, csvPath):
        self.init = init
        self.code_list = []
        self.batch_api = batchAPI
        self.conf_path = confPath
        self.log_path = logPath
        self.csv_path = csvPath


    def setBeginEndDay(self, beginDay, endDay):
        self.begin_day = beginDay
        self.end_day = endDay

        if self.init == True:
            finSH = open(self.conf_path+"/SH.code")
            codeSH = finSH.readlines()
            finSH.close()
            for code in codeSH: self.code_list.append('0'+code.strip('\n'))
            finSZ = open(self.conf_path+"/SZ.code")
            codeSZ = finSZ.readlines()
            finSZ.close()
            for code in codeSZ: self.code_list.append('1'+code.strip('\n'))
        else:
            fin = open(self.log_path+"/valid.code")
            valCodes = fin.readlines()
            fin.close()
            for code in valCodes: self.code_list.append(code.strip('\n'))


    def _getURL(self, dest_url, dest_file, retries=3):
        try:
            urllib.urlretrieve(dest_url, dest_file)
        except Exception,e:
            if retries > 0:
                return self._getURL(dest_url, dest_file, retries-1)
            else:
                logging.warning("Failed to download data for stock of [%s]." % self.cur_code)
                return -1
        logging.info("Download quotation data for stock of [%s] from [%s] to [%s]." % (self.cur_code, self.begin_day, self.end_day))
        return 0


    def crawlingData(self):
        for code in self.code_list:
            self.cur_code = code
            try:
                cur_url = self.batch_api + "code=" + code + "&start=" + self.begin_day + "&end=" + self.end_day
            except Exception,e:
                logging.warining("Please set the 'begin_day' and 'end_day' for data crawling.")
            dest_file = self.csv_path + '/history/' + code + ".csv"
            self._getURL(cur_url, dest_file)
            time.sleep(0.2)
        urllib.urlcleanup()



if __name__ == "__main__":
    batchAPI = "http://quotes.money.163.com/service/chddata.html?"
    dc = dataCrawl(True, batchAPI, "../../conf", "../../log", "../../data/")
    dc.setBeginEndDay("19900101", "20160120")
    dc.crawlingData()
