#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: raosiwei
# date: 2016-01-13

import sys,logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
import pymongo


class MA():

    # MA_n = avg(close(n))

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
        
        for result in results:
            quotes.append([result['close_price'], result['date']])
        return quotes


    def _getMA(self, period, quotes):
        prices = map(lambda x: x[0], quotes)
        dates = map(lambda x: x[1], quotes)

        length = len(prices)
        ma = [0] * length
        idx = 0
        while idx < length and idx+period < length:
            ma[idx] = round(sum(prices[idx:idx+period])/period, 3)
            idx += 1
        return zip(dates, ma)

    
    def calMA(self, period, num):
        if self.init == False: num = num + period
        if num > 240: num = 240
        quotes = self._getQuotation(num)
        if len(quotes) < period:
            logging.warning("There are not enough(at least %d-days) quotation data for BOLL calculation of [%s]" % (period, self.coll.name))
            return -1
            
        results = self._getMA(period, quotes)
        maStr = "MA_" + str(period)
        length = len(results)
        if self.init == False:
            length = length - period
        for i in range(length):
            self.coll.update_one({"date":results[i][0]}, {"$set":{maStr:results[i][1]}})
        logging.info("Calculate %d-MA for stock of [%s] successfully." % (period, self.coll.name))
        return 0


if __name__ == "__main__":
    conn = pymongo.MongoClient("127.0.0.1", 10001)
    db = conn.istock
    coll = db.stock_601398
    ma = MA(coll, True)
    quotes = ma._getQuotation(20)
    result = ma._getMA(10, quotes)
    for i in range(3):
        sys.stdout.write("%s\tMA=%.3f\n" % (result[i][0], result[i][1]))
    conn.close()
