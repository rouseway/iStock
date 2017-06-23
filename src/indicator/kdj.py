#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: raosiwei
# date: 2016-01-13

import sys,logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
import pymongo


class KDJ():

		# RSV=(close-low(9))/(high(9)-low(9))*100

		# K=(pre_K*2 + RSV)/3
		# D=(pre_D*2 + RSV)/3
		# J=3*K-2*D

    def __init__(self, coll, flag):
        self.coll = coll
        self.init = flag


    def _getQuotation(self, num):
        quotes = []
        try:
            results = list(self.coll.find().limit(num).sort("date", pymongo.DESCENDING))
        except Exception,e:
            logging.warning("Load quotation data for stock of [%s] failed." % self.coll.name)
            return quotes
        
        if self.init == True:
            for result in results:
                quotes.append([result['high_price'], result['low_price'], result['close_price'], result['date']])
        else:
            for i in range(len(results)):
                result = results[i]
                if i < num-9:
                    quotes.append([result['high_price'], result['low_price'], result['close_price'], result['date']])
                else:
                    quotes.append([result['high_price'], result['low_price'], result['close_price'], result['date'],\
                            result['KDJ_K'], result['KDJ_D']])
        return quotes
	

    def _getMA(self, quotes, array, kdType):
        ma = [0] * len(array)
        idx = len(array) - 1
        if self.init == True:
            ma[idx] = round((50*2 + array[idx])/3, 3)
        elif kdType == 0: # K-value
            ma[idx] = round((quotes[idx+1][4]*2 + array[idx])/3, 3)
        elif kdType == 1: # D-value
            ma[idx] = round((quotes[idx+1][5]*2 + array[idx])/3, 3)
        idx -= 1
        while idx >= 0:
            ma[idx] = round((ma[idx+1]*2 + array[idx])/3, 3)
            idx -= 1
        return ma
		
		
    def _getRSV(self, array):
        rsv = []
        dates = []
        idx = 0
        while idx < len(array) and idx+9 < len(array):
            high = max(map(lambda x: x[0], array[idx:idx+9]))
            low = min(map(lambda x: x[1], array[idx:idx+9]))
            close = array[idx][2]
            rsv.append(round((close-low)/(high-low)*100, 3))
            dates.append(array[idx][3])
            idx += 1
        return dates, rsv
		
		
    def _getKDJ(self, quotes):
        dates, rsv = self._getRSV(quotes)
        k = self._getMA(quotes, rsv, 0)
        d = self._getMA(quotes, k, 1)
        j = map(lambda x: round(3*x[0]-2*x[1], 3), zip(k, d))
        return zip(dates, k), zip(dates, d), zip(dates, j)


    def calKDJ(self, num):
        if self.init == False: num = num + 9
        if num > 240: num = 240
        quotes = self._getQuotation(num)
        if len(quotes) < 9:
            logging.warning("There are not enough(at least 9-days) quotation data for KDJ calculation \
                    of [%s]" % self.coll.name)
            return -1

        results = self._getKDJ(quotes)
        length = len(results[0])
        for i in range(length):
            self.coll.update_one({"date":results[0][i][0]}, {"$set":{"KDJ_K":results[0][i][1], \
                    "KDJ_D":results[1][i][1], "KDJ_J":results[2][i][1]}})
        logging.info("Calculate KDJ for stock of [%s] successfully." % self.coll.name)
        return 0




if __name__ == "__main__":
    conn = pymongo.MongoClient("127.0.0.1", 10001)
    db = conn.istock
    coll = db.stock_601398
    kdj = KDJ(coll, True)
    quotes = kdj._getQuotation(100)
    result = kdj._getKDJ(quotes)
    for i in range(10):
        sys.stdout.write("%s\tK=%f, D=%f, J=%f\n" % (result[0][i][0], result[0][i][1], result[1][i][1], result[2][i][1]))
    conn.close()
