#!
import pymongo

conn = pymongo.MongoClient("127.0.0.1", 10001)
db = conn.istock
coll = db.get_collection("stock_1300369")
#coll.insert({"name":"test"})
#print coll
#db.create_collection("stock_0601398")
#coll = db.stock_601398
#print coll.name
#print coll.count()
results = list(coll.find().limit(10).sort("date", -1)) #pymongo.DESCENDING))
print len(results)
for result in results:
    print "#",
    print result
    break

results = list(coll.find({"date":{"$gte":"2016-01-09", "$lte":"2016-01-22"}}).sort("date", -1))
print len(results)
for result in results:
    print result

conn.close()
