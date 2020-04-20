from crawler518.crawler518 import *
from datetime import date
from time import time

if __name__ == "__main__":
    start=time()
    HB_518 = crawler518()
    HB_518.start_crawl("富邦人壽")
    HB_518.resultdf = pretreat(HB_518.resultdf)
    try:
        HB_518.resultdf.to_excel('./result/HB_518_'+str(date.today())+'.xlsx',index=False)
    except:
        HB_518.resultdf.to_excel('./result/tmp.xlsx',index=False)
    print("time:"+str(time()-start))
    input('Press enter to exit')
