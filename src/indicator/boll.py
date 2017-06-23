#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: raosiwei
# date: 2016-01-13

import sys,logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
import pymongo


class BOLL():

		# MA=avg(close(20))
		# MD=std(close(20))
		# MB=MA(20)

        # UP=MB + 2*MD
		# DN=MB - 2*MD

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


    def _getMA(self, array):
        length = len(array)
        return sum(array) / length


    def _getMD(self, array):
        length = len(array)
        average = sum(array) / length
        d = 0
        for i in array: d += (i - average) ** 2
        return (d/length) ** 0.5


    def _getBOLL(self, quotes):
        prices = map(lambda x: x[0], quotes)
        dates = map(lambda x: x[1], quotes)
        
        length = len(prices)
        up = [0]*length
        mid = [0]*length
        low = [0]*length
        idx = 0
        while idx < length and idx+20 < length:
            md = round(self._getMD(prices[idx:idx+20]), 3)

            mid[idx] = round(self._getMA(prices[idx:idx+19]), 3)
            up[idx] = round(mid[idx] + 2*md, 3)
            low[idx] = round(mid[idx] - 2*md, 3)
            idx += 1
        return zip(dates, up), zip(dates, mid), zip(dates, low)


    def calBOLL(self, num):
        if self.init == False: num = num + 20
        quotes = self._getQuotation(num)
        if len(quotes) < 20:
            logging.warning("There are not enough(at least 20-days) quotation data for BOLL calculation of [%s]" % self.coll.name)
            return -1

        results = self._getBOLL(quotes)
        length = len(results[0])
        if self.init == False:
            length = length - 20
        for i in range(length):
            self.coll.update_one({"date":results[0][i][0]}, {"$set":{"BOLL_UP":results[0][i][1], "BOLL_MID":results[1][i][1], \
                    "BOLL_LOW":results[2][i][1]}})
        logging.info("Calculate BOLL for stock of [%s] successfully." % self.coll.name)
        return 0



if __name__ == "__main__":
    conn = pymongo.MongoClient("127.0.0.1", 10001)
    db = conn.istock
    coll = db.stock_601398
    boll = BOLL(coll, True)
    quotes = boll._getQuotation(100)
    result = boll._getBOLL(quotes)
    for i in range(10):
        sys.stdout.write("%s\tUP=%.3f, MID=%.3f, LOW=%.3f\n" % (result[0][i][0], result[0][i][1], \
                result[1][i][1], result[2][i][1]))
    conn.close()
