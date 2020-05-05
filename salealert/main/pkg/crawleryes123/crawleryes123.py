from selenium import webdriver
from bs4 import BeautifulSoup
from time import sleep
from re import findall
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import pandas as pd
from requests.packages.urllib3 import disable_warnings
disable_warnings()

class crawleryes123:
    def crawler(self,keyword):
        driverPath = "chromedriver.exe" 
        driver = webdriver.Chrome(driverPath)
        driver.implicitly_wait(30)
        driver.get("https://www.yes123.com.tw/admin/index.asp")
        search_input = driver.find_element_by_name("find_key1")
        search_input.send_keys(keyword)
        start_search_btn = driver.find_element_by_class_name("bt_search")
        start_search_btn.click()
        condition = EC.visibility_of_element_located((By.CLASS_NAME,'sift'))
        WebDriverWait(driver, 20, 0.5).until(condition)
        soup = BeautifulSoup(driver.page_source,"html.parser")
        s = findall("第.+頁",soup.text)[0]
        totalpage = int(findall('\d+',s)[1])
        current = int(findall('\d+',s)[0])

        url_list=[]
        for page in range(1,totalpage+1):
            print('page:'+str(page))
            while(current!=page):
                condition = EC.presence_of_element_located((By.NAME,"jump_page_num"))
                WebDriverWait(driver, 20, 0.5).until(condition)
                jump_page = driver.find_element_by_name("jump_page_num")
                jump_page.send_keys(page)
                
                condition = EC.element_to_be_clickable((By.CLASS_NAME,"bt_page"))
                #condition = EC.presence_of_element_located((By.CLASS_NAME,"bt_page"))
                WebDriverWait(driver, 20, 0.5).until(condition)
                jump_page_btn = driver.find_element_by_class_name("bt_page")
                jump_page_btn.click()
                
                condition = EC.visibility_of_element_located((By.CLASS_NAME,'sift'))
                WebDriverWait(driver, 20, 0.5).until(condition)
                soup = BeautifulSoup(driver.page_source,"html.parser")
                s = findall("第.+頁",soup.text)[0]
                current = int(findall('\d+',s)[0])
            for i in soup.find_all(class_='jobname'):
                if i.get('href'):
                    url_list.append('https://www.yes123.com.tw/admin/'+i['href'])
                    
                    
        result=[]
        for url in url_list:
            print(url)
            driver.get(url)
            condition = EC.visibility_of_element_located((By.CLASS_NAME,"comp_detail"))
            WebDriverWait(driver, 20, 0.5).until(condition)
            soup = BeautifulSoup(driver.page_source,"html.parser")
            
            url_company ='https://www.yes123.com.tw/admin/job_refer_comp_info.asp?'+findall('p_id.+&',url)[0][:-1]
            driver.get(url_company)
            condition = EC.visibility_of_element_located((By.CLASS_NAME,'company_title'))
            WebDriverWait(driver, 20, 0.5).until(condition)
            soup_c=BeautifulSoup(driver.page_source,"html.parser")
            company = soup_c.find_all(class_='company_title')[0].text
            result.append([url,soup,company])
            sleep(1)
        driver.close()
        return result
       
    def start_crawl(self,keyword):
        self.result = self.crawler(keyword)
        self.result2df()
       
       
    def result2df(self):
        def getcontent(soup):
            soup_ul = soup.find_all('ul')
            sub_title = []
            sub_content = []
            for i in soup_ul:
                tt = i.find_all(class_="tt")
                rr = i.find_all(class_="rr")
                if tt!=[] and rr!=[]:
                    for j in range(len(tt)):
                        sub_title.append(tt[j].text.replace(' ： ',''))
                        sub_content.append(rr[j].text.replace('\n','').replace('\t','').replace('\xa0','').replace('每月薪資行情表我要申訴','').replace(' ','').replace('地圖','').replace('就業導航',''))
            return dict(zip(sub_title,sub_content))

        colname = ['web','url','工作名稱','薪資','上班地點','上班時段','公司','工作性質','聯絡人']
        data = []
        for row in self.result:
            content = getcontent(row[1])
            value = ['yes123',
                     row[0],
                     row[1].find_all('h1')[0].text,
                     content['薪資待遇'],
                     content['工作地點'],
                     content['上班時段'],
                     row[2],
                     content['工作性質'],
                     content['連絡人'] ]
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

    