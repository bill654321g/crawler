from requests import Session
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from re import search,findall
from json import loads
from pandas import DataFrame
from requests.packages.urllib3 import disable_warnings
disable_warnings()

class crawler104:
    def __init__(self):
        self.headers_info = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "Cookie": "__auc=9457370616d4d50ee2a27100333; luauid=1453732090; _hjid=d8b876c6-505a-48f5-9be1-1afe372744d0; PERSONAL_SORT=A; SYS_SETAB=20140613; TS016ab800=01180e452d8a45044981d58f3bee2a49d0c4a23cc837b53ce8d0e1c383d5368e371fc32ffe5b1372d63c3b83a664efc7a20e3a1bbe; lup=1453732090.5035849152215.5001489413863.1.4640712161167; lunp=5001489413863; __asc=0554bdf61700f513fd79248de93; _gid=GA1.3.668400390.1580804948; _dc_gtm_UA-15276226-1=1; _gat_UA-15276226-1=1; _ga=GA1.1.1665531603.1568960213; _ga_FJWMQR9J2K=GS1.1.1580804947.3.0.1580804951.0; _ga_W9X1GB1SVR=GS1.1.1580804947.22.0.1580804951.56",
            "Host": "www.104.com.tw",
            "Referer": "https://www.104.com.tw/jobs/main/",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36"
                 }
        

        
    
    def crawler(self,keyword):
        res = Session()
        res.mount( 'https://', HTTPAdapter( max_retries = 5 ))
        params = {"keyword" : keyword,
                "ro": "0",
                "kwop": 7,
                "order": 15,
                "asc": 0,
                "page": 1,
                "mode": "s",
                "jobsource": "2018indexpoc"}
        res1 = res.get("https://www.104.com.tw/jobs/search",params = params,headers = self.headers_info,verify = False)
        soup = BeautifulSoup(res1.text,'html.parser')
        totalPage=int(search( r'\"{}\":(\d*)'.format("totalPage"), soup.text).group(1))
        result = []
        for page in range(1,totalPage+1):
            params['page']=page
            res1=res.get("https://www.104.com.tw/jobs/search",params = params,headers = self.headers_info,verify = False)
            soup = BeautifulSoup(res1.text,'html.parser')
            
            soup = soup.find_all("a",class_="js-job-link")
            for s in soup:
                jobid = s['href'].split('?')[0].split('/')[-1]
                print('https://www.104.com.tw/job/'+jobid)
                res2 = res.get('https://www.104.com.tw/job/ajax/content/'+jobid,headers = self.headers_info,verify = False)
                data = loads(res2.text)['data']
                try:
                    if keyword in data['header']['custName']:
                        result.append(['https://www.104.com.tw/job/'+jobid,data])
                except:
                    print('')
        return result
        
    def start_crawl(self,keyword):
        self.result = self.crawler(keyword)
        self.result2df()
        
       
    def result2df(self):
        colname = ['url','工作名稱','薪資','上班地點','上班時段','公司','工作性質','聯絡人']
        data = []
        for row in self.result:
            value = [row[0],
                     row[1]['header']['jobName'],
                     row[1]['jobDetail']['salary'],
                     row[1]['jobDetail']['addressRegion']+row[1]['jobDetail']['addressDetail'],
                     row[1]['jobDetail']['workPeriod'],
                     row[1]['header']['custName'],
                     row[1]['jobDetail']['jobType'],
                     row[1]['contact']['hrName']
                    ]
            r = dict(zip(colname,value))
            data.append(r)
        df = DataFrame(data)
        self.resultdf=df
        
def pretreat(df):
    df.loc[df.工作性質==1,'工作性質'] = '正職'
    df.loc[df.工作性質==2,'工作性質'] = '兼職'
    
    def work_content(s):
        for e in ["保險業務員","壽險顧問","保險儲備主管","理財"]:
            if e in s:
                return False
            else:
                return True

#    def work_time(s):
#        pattern="\d+:\d+"
#        r=re.findall(pattern, s, flags=0)
#        if len(r) == 2:
#            if (int(r[1].replace(":",""))-int(r[0].replace(":","")))<800:
#                return False
#            else:
#                return True
#        else:
#            return True
#        
    def work_salary(s):
        return (s == '時薪158元' or s == '待遇面議')

    def work_com_num(s):
        if '核准文號' in s:
            s=s[s.find('核准文號'):]
            patern="[\dA-Z]+"
            r=findall(patern,s)
            if r:
                if r[0] not in ["109A01","109A02","109A03"]:
                    return False
                else:
                    return True
            else:
                return True
        else :
            return True

    def work_part_time(s):
        if s =='正職':
            return True
        else:
            return False


    
    df["是否違規"] = ~(df["工作名稱"].apply(work_content)&
                       df['薪資'].apply(work_salary)&
                       df["公司"].apply(work_com_num)&
                       df["工作性質"].apply(work_part_time))#&df["上班時段"].apply(work_time)

    df['違規原因'] = ""
    df.loc[~df["工作名稱"].apply(work_content),'違規原因'] += "、工作名稱"
    df.loc[~df['薪資'].apply(work_salary),'違規原因'] += "、薪資錯誤"
    df.loc[~df["公司"].apply(work_com_num),'違規原因'] += "、核准文號錯誤"
    df.loc[~df["工作性質"].apply(work_part_time),'違規原因'] += "、工作性質錯誤"
    #df.loc[~df["上班時段"].apply(work_time),'違規原因']="上班時段錯誤(未滿8小時的時間)"
    
    def rmtmp(x):
        if len(x)>0:
            if x[0] =='、':
                return x.replace('、','',1)
        else:
            return x
    df.loc[:,'違規原因'] = df['違規原因'].apply(rmtmp)
    df = df[df['是否違規']]
    df = df[df['公司'].apply(lambda x: '總公司' not in x)]
    return df

    
    