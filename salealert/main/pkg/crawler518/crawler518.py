from requests import Session
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from re import findall
import pandas as pd
from requests.packages.urllib3 import disable_warnings
disable_warnings()

class crawler518:
    def crawler(self,keyword):
        res = Session()
        res.mount( 'https://', HTTPAdapter( max_retries = 5 ))
        params = {"ad" : keyword,'aa':'','ab':'','ac':'','am':'','i':''}

        headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
           "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
           'Connection':'close'}

        res1 = res.get("https://www.518.com.tw/job-index.html",params = params,headers = headers)
        soup = BeautifulSoup(res1.text,'html.parser')
        totalPage=int(findall('\d+',soup.find_all(class_="pagecountnum")[0].text)[1])
        url_list = []
        for s in soup.find_all(class_="title"):
            url_list.append(s.a['href'])
        if totalPage>1:
            params['i']=1
            params['am']=1
            params['ai']=0
            params = {"ad" : keyword,'i':1,'am':1,'ai':0}
            for page in range(2,totalPage+1):
                res2 = res.get("https://www.518.com.tw/job-index-P-"+str(page)+".html",params = params,headers = headers)
                soup1 = BeautifulSoup(res2.text,'html.parser')
                for s in soup1.find_all(class_="title"):
                    url_list.append(s.a['href'])
        result=[]
        for url in url_list:
            res2 = res.get(url,headers = headers)
            soup2 = BeautifulSoup(res2.text,'html.parser')
            print(url)
            result.append([url,soup2])
        return result
       
    def start_crawl(self,keyword):
        self.result = self.crawler(keyword)
        self.result2df()
       
       
    def result2df(self):
        def getcontent(soup):
            for cl in ["jobItem","job-detail-box"]:
                soup_ul = soup.find_all(class_=cl)
                sub_title = []
                sub_content = []
                for i in soup_ul:
                    tt = i.find_all('li')
                    if tt!=[]:
                        for j in range(len(tt)):
                            if tt[j].find_all('span')!=[]:
                                sub_title.append(tt[j].text.split(" ")[0].replace('：','').replace(' ','').replace('\u3000',''))
                                sub_content.append(tt[j].find_all('span')[0].text.replace(' ','').split('\n')[0].replace('地圖',''))
            return dict(zip(sub_title,sub_content))

        colname = ['web','url','工作名稱','薪資','上班地點','上班時段','公司','工作性質','聯絡人']
        data = []
        for row in self.result:
            content = getcontent(row[1])
            value = ['518',
                             row[0],
                             row[1].title.text.split('-')[0],
                             content['薪資待遇'],
                             content['上班地點'],
                             content['上班時段'],
                             row[1].find_all(class_="company-info")[0].a['title'],
                             content['工作性質'],
                             content['職務聯絡人'],
                        ]
            r = dict(zip(colname,value))
            data.append(r)
        df = pd.DataFrame(data)
        self.resultdf=df


def pretreat(df):
    df.loc[df.工作性質==1,'工作性質'] = '正職'
    df.loc[df.工作性質==2,'工作性質'] = '兼職'
    def strQ2B(ustring):
        """把字串全形轉半形"""
        ss = []
        for s in ustring:
            rstring = ""
            for uchar in s:
                inside_code = ord(uchar)
                if inside_code == 12288:  # 全形空格直接轉換
                    inside_code = 32
                elif (inside_code >= 65281 and inside_code <= 65374):  # 全形字元（除空格）根據關係轉化
                    inside_code -= 65248
                rstring += chr(inside_code)
            ss.append(rstring)
        return ''.join(ss)
    def work_content(s):
        if strQ2B(s).replace(' ','').replace('行','行') in ['展業員NCT(正職)','行銷專員CA(正職)','行銷專員PMS(正職)','【業務通路】行銷專員CA(正職)','行銷專員(正職)','(正職)行銷專員CA','行銷專員(正職）','行銷專員CA','展業員「全職」','行銷專員CA(正職)(需求人數:不拘)','行銷專員CA(正職','展業員','行銷專員(正職)','【業務通路】行銷專員CA','行銷專員CA','行銷專員CA正職','行銷專員','展業員NCT','行銷專員-正職','行銷專員CA-正職','展業員NCT正職','行銷專員(CA)','展業員NCT正職']:
            return True
        else:
            return False
#        for e in ["保險業務員","壽險顧問","保險儲備主管","理財"]:
#            if e in s:
#                return False
#            else:
#                return True
 
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
        return s == '時薪158元'
 
    def work_com_num(s):
        s=s.replace('淮','准')
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
        if s =='正職' or s =='全職':
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
    df = df[df['公司'].apply(lambda x: '總公司' not in x)]
    t=str(df.shape[0])
    #df = df[df['是否違規']]
    print('total:'+t)
#    df = df[df['公司'].apply(lambda x: '總公司' not in x)]
    return df    