#from crawler104Modules import Parser104
from crawler104Core import Crawler104Core
import settings
import pandas
import datetime
import os
import re
from xml.etree import ElementTree as xml_etree_ElementTree

import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
import time
import numpy as np


class Crawler104:
	def __init__(self):
		self.crawlerCore = Crawler104Core()
		self.df_unparsed = pandas.DataFrame()

	def get_filepath(self, name):
		filename = "jobs104_"+ str(datetime.datetime.now().date()).replace("-","")+"_" + name
		filename += ".xlsx"
		filename = os.path.join(settings.dataDirectory,filename)
		return filename

	def start_crawl(self, url):
		df = pandas.DataFrame(self.crawlerCore.start_crawl(url))
		self.df_unparsed = self.df_unparsed.append(df)

	def generate_excel(self,filename):
		self.df_unparsed.to_excel(self.get_filepath(filename),index=False)

	def parse_unparsed_excel(self,from_filepath,sheet="Sheet1"):
		parser104 = Parser104(configPath,appliedNumberSheet,workingAreaSheet)
		df = pandas.read_excel(io=from_filepath,sheet_name=sheet)
		df = parser104.parse104Dataframe(df)
		
		filename = os.path.basename(from_filepath) + "_parsed" + ".xlsx"
		to_filepath = os.path.join(settings.parsedDirectory,filename)
		df.to_excel(to_filepath,index=False)
		print("finished parsing:  {} -> {}".format(from_filepath,to_filepath))

	def pretreat(self):
		df=self.df_unparsed
		df=df[df['公司'].apply(lambda x : "富邦" in x)]
		df=df[df['工作內容'].apply(lambda x : "銷售" in x)]
		col=['上班地點', '上班時段', '公司', '工作內容','工作名稱', '工作待遇', '工作連結','聯絡人']
		df=df[col]
		def work_content(s):
			for e in ["保險業務員兼職人員","理財專員","理財規劃師","理財"]:
				if e in s:
					return False
				else:
					return True

		def work_time(s):
			pattern="\d+:\d+"
			r=re.findall(pattern, s, flags=0)
			if r:
				if (int(r[1].replace(":",""))-int(r[0].replace(":","")))<800:
					return False
				else:
					return True
			else:
				return True

		def work_salary(s):
			patern="\d+,*\d+"
			r=re.findall(patern,s)
			if r:
				for n in r:
					if int(n.replace(",",""))>40000:
						return False
				return True
			else:
				return True
		def work_loc(s):
			if "敦化南路一段" in s:
				return False
			else:
				return True

		def work_com_num(s):
			if '核准文號' in s:
				patern="[\dA-Z]+"
				r=re.findall(patern,s)
				if r:
					if r[0] != "108A01":
						return False
					else:
						return True
				else:
					return True
			else :
				return True
		df["是否違規"]=~(df["工作名稱"].apply(work_content)&df["上班時段"].apply(work_time)&df['工作待遇'].apply(work_salary)&df['上班地點'].apply(work_loc)&df["公司"].apply(work_com_num))
		df['違規原因']=np.nan
		df.loc[~df["工作名稱"].apply(work_content),'違規原因']="工作名稱"
		df.loc[~df["上班時段"].apply(work_time),'違規原因']="上班時段錯誤(未滿8小時的時間)"
		df.loc[~df['工作待遇'].apply(work_salary),'違規原因']="薪資福利錯誤"
		df.loc[~df['上班地點'].apply(work_loc),'違規原因']="上班地點錯誤(總公司)"
		df.loc[~df["公司"].apply(work_com_num),'違規原因']="核准文號錯誤"
		self.df_unparsed = df


if __name__ == "__main__":
	start=time.time()
	# url = url + &[Parameter]=[Value]%2C[Value]%2C[Value]
	# 參數:         ro 工作型態, jobcat 職務類別, area 地區, indcat 公司產業, keyword 關鍵字搜尋
	# 額外參數:     order 排序方式, asc 由低到高
	ro_dict = {"全部":"0","全職":"1","兼職":"2","高階":"3","派遣":"4","接案":"5","家教":"6" }
	order_dict = {"符合度排序":"12","日期排序":"11","學歷":"4","經歷":"3","應徵人數":"7","待遇":"13"}
	root_url = 'https://www.104.com.tw/jobs/search/?ro=0&kwop=7&keyword=富邦人壽&order=14&asc=0&page={}&mode=s&jobsource=2018indexpoc'

	myCrawler = Crawler104()
	myCrawler.start_crawl(root_url)
	myCrawler.pretreat()
	myCrawler.generate_excel("test")
	print("time:"+str(time.time()-start))
	#myCrawler.parse_unparsed_excel("../data/jobs104_20190912_IT產業.xlsx")

