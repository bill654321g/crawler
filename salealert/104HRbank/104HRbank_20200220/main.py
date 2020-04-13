from crawler104.crawler104 import *
from datetime import date
from time import time

if __name__ == "__main__":
    start=time()
    bank104 = crawler104()
    bank104.start_crawl("富邦人壽")
    bank104.resultdf = pretreat(bank104.resultdf)
    bank104.resultdf.to_excel('./result/result'+str(date.today())+'.xlsx',index=False)
    print("time:"+str(time()-start))
    input('Press enter to exit')
