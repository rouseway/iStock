
import datetime

a = "2016-01-15"
#a = '0'
date_a = datetime.datetime.strptime(a, "%Y-%m-%d")
date_b = date_a + datetime.timedelta(days=1)

print date_a.strftime("%Y%m%d")
print date_a
print date_b

def testExp():
    try:
        a = 1/0
    except Exception,e:
        print "divide zero"
        return -1
    return 0

if __name__ == "__main__":
    ret = testExp()
    print ret
    fout = open("./test.txt", 'w')
    fout.write("%s\n" % "hello")
    fout.close()
