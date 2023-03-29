#https://employment.en-japan.com/

from bs4 import BeautifulSoup
import requests
import joblib
from tqdm import tqdm
import pandas as pd

def joblib_get_url(i):
    url_list_pre_2=list()
    url=url_list_001[i]
    raw_url="https://employment.en-japan.com/"

    res=requests.get(url)
    soup=BeautifulSoup(res.text,"html.parser")

    for i in range(0,50):
        try:
            elem=soup.find_all("div",class_="list")[i].find("div",class_="jobSearchListUnit").find("div").find("div",class_="jobNameArea").find("a")
            url_key=elem.attrs["href"]
            n_url_key=raw_url+url_key
            url_list_pre_2.append(n_url_key)
        except:
            pass
    return url_list_pre_2
    

def joblib_get_data(i):
    url=url_list_003[i]
    res=requests.get(url)
    soup=BeautifulSoup(res.text,"html5lib")

    new_list=list()
    #法人名 支店名 法人住所 支店住所 従業員数 業種 職種 雇用形態
    houjin=shiten=houjin_addr=shiten_addr=members=Industry=job_type=employee=" "

    #法人名
    try:
        
        selector="#descCompanyName > div.base > div.company > span.text"
        raw_houjin=soup.select_one(selector).get_text()
        houjin=raw_houjin.split()
    except:
        pass

    #業種
    try:
        selector="#globalPankuzu > ol > li:nth-of-type(2) > a > span"
        raw_Industry=soup.select_one(selector).get_text()
        Industry=raw_Industry
    except:
        pass

    #雇用形態
    for i in range(0,10):
        try:
            juge=soup.find("div",class_="descArticleUnit dataWork").find("div",class_="contents").find("table").find("tbody").find_all("tr")[i].find("th").get_text()
            if '雇用形態' in juge:
                raw_employee=soup.find("div",class_="descArticleUnit dataWork").find("div",class_="contents").find("table").find("tbody").find_all("tr")[i].find("td").get_text()
                employee=raw_employee.split("<br>")
        except:
            pass
    
    #従業員数
    for i in range(0,10):
        try:
            juge=soup.find("div",class_="descArticleArea descSubArticle").find("div",class_="descArticleUnit dataCompanyInfoSummary").find("div",class_="contents").find("table",class_="dataTable").find("tbody").find_all("tr")[i].find("th").get_text()
            if "従業員数" in juge:
                raw_members=soup.find("div",class_="descArticleArea descSubArticle").find("div",class_="descArticleUnit dataCompanyInfoSummary").find("div",class_="contents").find("table",class_="dataTable").find("tbody").find_all("tr")[i].find("td").get_text()
                members=raw_members
        except:
            pass
    #法人住所
    for i in range(0,10):
        try:
            juge=soup.find("div",class_="descArticleUnit dataCompanyInfoSummary").find("div",class_="contents").find("table").find("tbody").find_all("tr")[i].find("th").get_text()
            if "事業所" in juge:
                raw_houjin_addr=soup.find("div",class_="descArticleUnit dataCompanyInfoSummary").find("div",class_="contents").find("table").find("tbody").find_all("tr")[i].find("td").get_text()
                houjin_addr=raw_houjin_addr
        except:
            pass
    #職種
    for i in range(0,10):
        try:
            juge=soup.find("div",class_="descArticleUnit dataCompanyInfoSummary").find("div",class_="contents").find("table").find("tbody").find_all("tr")[i].find("th").get_text()
            if "事業内容" in juge:
                raw_job_type=soup.find("div",class_="descArticleUnit dataCompanyInfoSummary").find("div",class_="contents").find("table").find("tbody").find_all("tr")[i].find("td").get_text()
                job_type=raw_job_type
        except:
            pass    

    new_list.append(houjin[0])
    new_list.append(houjin_addr)
    new_list.append(members)
    new_list.append(Industry)
    new_list.append(job_type)
    new_list.append(employee[0])

    return new_list



url="https://employment.en-japan.com/search/search_list/?areaid=1_2_3_4_5_6_7_8&aroute=0&arearoute=1"
#https://employment.en-japan.com/search/search_list/?areaid=1_2_3_4_5_6_7_8&pagenum=2&aroute=0&arearoute=1
res=requests.get(url)
soup=BeautifulSoup(res.text,"html.parser")

selector="#jobSearchListNum > div.num > em"
all_num=soup.select_one(selector).get_text()
all_num=int(all_num)

if all_num%50 == 0:
    total_page_num=all_num//50
else:
    total_page_num=all_num//50+1

url_list_001=list()
url_list_001.append(url)
list_pre=list()

for i in range(2,total_page_num+1):
    list_pre="https://employment.en-japan.com/search/search_list/?areaid=1_2_3_4_5_6_7_8&pagenum="+str(i)+"&aroute=0&arearoute=1"
    url_list_001.append(list_pre)

url_list_002=list()

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
enjapan_df=pd.DataFrame(all_list_filtered,columns=["法人名","法人住所","従業員数","業種","職種","雇用形態"])
enjapan_df.to_csv("enjapan_data_test_100.csv",encoding="utf-8-sig")
