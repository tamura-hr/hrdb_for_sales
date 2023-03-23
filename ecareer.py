from bs4 import BeautifulSoup
import requests
import joblib
from tqdm import tqdm
import pandas as pd

#ecareer

def joblib_get_url(i):
    url_list_pre=list()
    url=url_list_001[i]
    raw_url = "https://www.ecareer.ne.jp"

    res=requests.get(url)
    soup=BeautifulSoup(res.text,"html.parser")

    for i in range(0,30):
        try:
            elem=soup.find_all("div",class_="wrapBox")[i].find("header").find("h2").find("a")
            url_key=elem.attrs["href"]
            n_url_key=raw_url+url_key
            url_list_pre.append(n_url_key)
        except:
            pass
    return url_list_pre

def joblib_get_data(i):
    new_list=list()
    #法人名 支店名 法人住所 支店住所 従業員数 業種 職種 雇用形態
    houjin=shiten=houjin_addr=shiten_addr=members=Industry=job_type=employee=" "

    url=url_list_003[i]
    res=requests.get(url)
    soup=BeautifulSoup(res.text,"html.parser")

    #法人名
    for i in range(10):
      try:
        judge=soup.find("div",class_="corpInfo").find("table").find("tbody").find_all("tr")[i].find("th").get_text()
        if "企業名" in judge:
            raw_houjin=soup.find("table",class_="corpDetail").find("tbody").find_all("tr")[i].find("td").get_text()
            houjin=raw_houjin
      except:
        pass    
    
    #法人住所  
    for i in range(10):
      try:
        judge=soup.find("div",class_="corpInfo").find("table").find("tbody").find_all("tr")[i].find("th").get_text()
        if "所在地" in judge:
            raw_houjin_addr=soup.find("table",class_="corpDetail").find("tbody").find_all("tr")[i].find("td").get_text()
            houjin_addr=raw_houjin_addr
      except:
        pass
    
    #従業員数
    for i in range(10):
      try:
        judge=soup.find("div",class_="corpInfo").find("table").find("tbody").find_all("tr")[i].find("th").get_text()
        if "従業員数" in judge:
            raw_members=soup.find("table",class_="corpDetail").find("tbody").find_all("tr")[i].find("td").get_text()
            members=raw_members.split()
      except:
        pass
    
    #職種
    for i in range(10):
       try:
          judge=soup.find("div",class_="corpInfo").find("table").find("tbody").find_all("tr")[i].find("th").get_text()
          if "事業内容" in judge:
            raw_job_type=soup.find("table",class_="corpDetail").find("tbody").find_all("tr")[i].find("td").get_text()
            job_type=raw_job_type
       except:
          pass
      
    #業種 #wrapper > div:nth-child(3) > ul > li:nth-child(2) > a
    try:
        selector_second="#wrapper > div:nth-of-type(2) > ul > li:nth-of-type(2) > a"#nth-child(1)はサポートされていないCSSセレクタなのでnth-of-type(1)に変換する必要がある
        raw_Industry=soup.select_one(selector_second).get_text()
        Industry=raw_Industry.split()
    except:
        pass       

    #雇用形態
    for i in range(10):
       try:
          judge=soup.find("div",class_="detail").find("div",class_="content").find("table").find("tbody").find_all("tr")[i].find("th").get_text()
          if "雇用形態" in judge:
            raw_employee=soup.find("div",class_="detail").find("div",class_="content").find("table").find("tbody").find_all("tr")[i].find("td").get_text()
            employee=raw_employee.split()
       except:
          pass
    
    #支店住所
    for i in range(10):
       try:
          judge=soup.find("div",class_="detail").find("div",class_="content").find("table").find("tbody").find_all("tr")[i].find("th").get_text()
          if "勤務地" in judge:
            raw_shiten_addr=soup.find("div",class_="detail").find("div",class_="content").find("table").find("tbody").find_all("tr")[i].find("td").get_text()
            shiten_addr=raw_shiten_addr
       except:
          pass    
       
    new_list.append(houjin)
    new_list.append(houjin_addr)
    new_list.append(shiten_addr)
    new_list.append(members[0]+"人")
    new_list.append(Industry[0])
    new_list.append(job_type)
    new_list.append(employee[0])
    

    return new_list


url="https://www.ecareer.ne.jp/positions"
res=requests.get(url)
soup=BeautifulSoup(res.text,"html.parser")

selector="#searchForm > div.inner > header > span"
total_num=soup.select_one(selector).get_text()
total_num=int(total_num.replace(",",""))

n=30

if total_num%n==0:
    total_page_num=total_num//30
else:
    total_page_num=total_num//30 +1

url_list_001=list()
url_list_001.append(url)

for i in range(2,total_page_num):
    url="https://www.ecareer.ne.jp/positions?window="+str(i)
    url_list_001.append(url)

url_list_002=list()

'''
a=100
joblib_num = total_page_num//a + 1

'''
a=1#テスト用
joblib_num=10 #テスト用

for n in tqdm(range(0, joblib_num)):
    try:
        resultList = joblib.Parallel(n_jobs=12, verbose=3)( [joblib.delayed(joblib_get_url)(i) for i in range(n*a,(n+1)*a) ])
        url_list_002.extend(resultList)
    except:
        pass

url_list_002_filtered=[x for x in url_list_002 if x is not None]
'''
for x in url_list_002:
  if x is not None
'''
flatten_url_list_002 = [ flatten for inner in url_list_002_filtered for flatten in inner ]

url_list_003 = list()
for i in flatten_url_list_002:
  if i not in url_list_002:
      url_list_003.append(i)




b=1#テスト用
joblib_num=100#テスト用

all_list=list()

for n in tqdm(range(0,joblib_num)):
    try:
        resultList = joblib.Parallel(n_jobs=12, verbose=3)( [joblib.delayed(joblib_get_data)(i) for i in range(n*b,(n+1)*b) ])
        #resultList = joblib.Parallel(n_jobs=12, verbose=3)( [joblib.delayed(joblib_get_data)(i) ])
        all_list.extend(resultList)
    except:
        pass

all_list_filtered=[x for x in all_list if x is not None]
ecareer_df=pd.DataFrame(all_list_filtered,columns=["法人名","法人住所","支店住所","従業員数","業種","職種","雇用形態"])
ecareer_df.to_csv("ecareeer_data_test_100.csv",encoding="utf-8-sig")