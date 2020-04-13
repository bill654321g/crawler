from crawleryes123.crawleryes123 import *
from datetime import date
from time import time

if __name__ == "__main__":
    start=time()
    yes123 = crawleryes123()
    yes123.start_crawl("富邦人壽")
    yes123.resultdf = pretreat(yes123.resultdf)
    try:
        yes123.resultdf.to_excel('./result/result'+str(date.today())+'.xlsx',index=False)
    except:
        yes123.resultdf.to_excel('./result/tmp.xlsx',index=False)
    print("time:"+str(time()-start))
    input('Press enter to exit')
