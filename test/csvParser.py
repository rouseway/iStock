#!
import csv
import pymongo


csvfile = file('./600406.csv', 'rb')
#print len(csvfile.readlines())
reader = list(csv.reader(csvfile))
print len(reader)
for rd in reader:
    print rd[2]
    uname = rd[2].decode("gbk")
    print uname
    print uname.encode("utf-8")
    break
csvfile.close()

"""
conn = pymongo.MongoClient("127.0.0.1", 10001)
db = conn.istock

cnt = 0
for oneDay in reader:
    if cnt == 0:
        cnt += 1
        continue
    if float(oneDay[6]) != 0:
        oneRecord = dict(date=oneDay[0], close_price=float(oneDay[3]), high_price=float(oneDay[4]), \
                low_price=float(oneDay[5]), open_price=float(oneDay[6]), KDJ_K=0, KDJ_D=0, KDJ_J=0)
        db.stock_600406.insert(oneRecord)
        print oneDay

csvfile.close()
conn.close()
"""
