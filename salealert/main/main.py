from pkg.crawleryes123.crawleryes123 import *
from pkg.crawler104.crawler104 import *
from pkg.crawler518.crawler518 import *
from datetime import date
from time import time

if __name__ == "__main__":
    hb_104 = input('Do you want to crawler 104 HR bank(y/n)?')
    if hb_104=='y':
        t0 = time()
        bank104 = crawler104()
        bank104.start_crawl("富邦人壽")
        bank104.resultdf = pretreat(bank104.resultdf)
        try:
            bank104.resultdf.to_excel('./result/hb_104_'+str(date.today())+'.xlsx',index=False,sheet_name='104')
        except:
            bank104.resultdf.to_excel('./result/hb_104_'+str(date.today())+'_tmp.xlsx',index=False,sheet_name='104')
        t1 = time()
        print("time:"+str(t1-t0))

    hb_518 = input('Do you want to crawler 518 HR bank(y/n)?')
    if hb_518=='y':
        t0 = time()
        HB_518 = crawler518()
        HB_518.start_crawl("富邦人壽")
        HB_518.resultdf = pretreat(HB_518.resultdf)
        try:
            HB_518.resultdf.to_excel('./result/hb_518_'+str(date.today())+'.xlsx',index=False,sheet_name='518')
        except:
            HB_518.resultdf.to_excel('./result/hb_518_'+str(date.today())+'_tmp.xlsx',index=False,sheet_name='518')
        t1 = time()
        print("time:"+str(t1-t0))
    hb_yes123 = input('Do you want to crawler yes123 HR bank(y/n)?')
    if hb_yes123=='y':
        t0 = time()
        yes123 = crawleryes123()
        yes123.start_crawl("富邦人壽")
        yes123.resultdf = pretreat(yes123.resultdf)
        try:
            yes123.resultdf.to_excel('./result/hb_yes123_'+str(date.today())+'.xlsx',index=False,sheet_name='yes123')
        except:
            yes123.resultdf.to_excel('./result/hb_yes123_'+str(date.today())+'_tmp.xlsx',index=False,sheet_name='yes123')
        t1 = time()
        print("time:"+str(t1-t0))
    input('Press enter to exit')
