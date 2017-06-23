#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: raosiwei
# date: 2016-01-13

import sys,logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
import pymongo


class MACD():

		# EMA(12)=PreEMA(12)* 11/13 + Close * 2/13
		# EMA(26)=PreEMA(26)* 25/27 + Close * 2/27
		
		# DIF=EMA(12)-EMA(26)
		# DEA=PreDEA * 8/10 + DIF * 2/10
		# MACD=(DIF-DEA) * 2

    def __init__(self, coll, flag):
        self.coll = coll
        self.init = flag
	

    def _getQuotation(self):
        quotes = []
        try:
            results = list(self.coll.find().sort("date", pymongo.DESCENDING))
        except Exception,e:
            logging.warning("Load quotation data for stock of [%s] failed." % self.coll.name)
            return quotes
        
        for result in results:
            quotes.append([result['close_price'], result['date']])
        quotes.reverse()
        return quotes

    
    def _getEMA(self, prices, n):
        ema = [0] * len(prices)
        ema[1] = round(prices[0] + (prices[1]-prices[0]) * 2/float(n+1), 4)
        for i in range(2, len(prices)):
            ema[i] = round(ema[i-1] * (n-1)/float(n+1) + prices[i] * 2/float(n+1), 4)
        return ema


    def _getDEA(self, diff):
        dea = [0] * len(diff)
        dea[1] = round(diff[1] * 2/10, 4)
        for i in range(2, len(diff)):
            dea[i] = round(dea[i-1]*0.8 + diff[i]*0.2, 4)
        return dea


    def _getMACD(self, quotes):
        prices = map(lambda x: x[0], quotes)
        dates = map(lambda x: x[1], quotes)
        short_ema = self._getEMA(prices, 12)
        long_ema = self._getEMA(prices, 26)
        diff = map(lambda x: round(x[0]-x[1], 4), zip(short_ema, long_ema))
        dea = self._getDEA(diff)
        bar = map(lambda x: round(2*(x[0]-x[1]), 4), zip(diff, dea))
        return zip(dates, diff), zip(dates, dea), zip(dates, bar)


    def calMACD(self, num):
        results = self._getQuotation()
        if len(results) < 26:
            logging.warning("There are not enough(at least 26-days) quotation data for MACD calculation of [%s]" % self.coll.name)
            return -1

        results = self._getMACD(results)
        length = len(results[0])
        idx = 0
        if self.init == False: idx = length-num-1
        while idx < length:
            self.coll.update_one({"date":results[0][idx][0]}, {"$set":{"MACD_DIF":results[0][idx][1], \
                    "MACD_DEA":results[1][idx][1], "MACD_BAR":results[2][idx][1]}})
            idx += 1
        logging.info("Calculate MACD for stock of [%s] successfully." % self.coll.name)
        return 0



if __name__ == "__main__":
    conn = pymongo.MongoClient("127.0.0.1", 10001)
    db = conn.istock
    coll = db.stock_601398
    macd = MACD(coll, True)
    quotes = macd._getQuotation()
    result = macd._getMACD(quotes)
    end = len(result[0])-1
    for i in range(10):
        sys.stdout.write("%s\tDIFF=%.3f, DEA=%.3f, BAR=%.3f\n" % (result[0][end-i][0], result[0][end-i][1], \
                result[1][end-i][1], result[2][end-i][1]))
    conn.close()
