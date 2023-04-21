#https://tenshoku.mynavi.jp/

from bs4 import BeautifulSoup
import requests
import joblib
from tqdm import tqdm
import pandas as pd
import re

def joblib_get_url(i):
    url_list_pre=list()
    url=url_list_001[i]
    raw_url="https://tenshoku.mynavi.jp"

    res=requests.get(url)
    soup=BeautifulSoup(res.text,"html.parser")

    for i in range(0,50):
        try:
            elem=soup.find_all("div",class_="cassetteRecruit__bottom")[i].find("a")
            url_key=elem.attrs["href"]
            n_url_key=raw_url+url_key
            n_url_key=n_url_key.replace("msg/","")
            url_list_pre.append(n_url_key)
        except:
            pass
    return url_list_pre    

def joblib_get_data(i):
    new_list=list()
    media=postcode=members_etc=houjin=shiten=houjin_addr=shiten_addr=members=Industry=job_type=employee=" "
    url=url_list_003[i]
    res=requests.get(url)
    soup=BeautifulSoup(res.text,"html.parser")

    #媒体名
    media="mynavi"

    #法人名
    try:
        selector="body > div.wrapper > div.container.container-jobinfo > div:nth-of-type(2) > div > div.cassetteOfferRecapitulate__content.cassetteOfferRecapitulate__content-jobinfo > div.blockWrapper > div.rightBlock > h1 > span.companyName"
        raw_houjin=soup.select_one(selector).get_text()
        houjin=raw_houjin.split()
    except:
        pass

    #業種
    try:
        selector="body > div.wrapper > div.breadcrumb > ul > li:nth-of-type(3) > a"    
        raw_Industry=soup.select_one(selector).get_text()
        Industry=raw_Industry.split()
    except:
        pass

    #雇用形態
    for i in range(0,10):
        try:
            juge=soup.find("table",class_="jobOfferTable").find("tbody").find_all("tr")[i].find("th").get_text()
            if "雇用形態" in juge:
                raw_employee=soup.find("table",class_="jobOfferTable").find("tbody").find_all("tr")[i].find("td").find("div").get_text()
                employee=raw_employee.split()
        except:
            pass
    #従業員数
    for i in range(0,10):
        try:
            juge=soup.find("table",class_="jobOfferTable thL").find("tbody").find_all("tr")[i].find("th").get_text()
            if "従業員数" in juge:
                raw_members=soup.find("table",class_="jobOfferTable thL").find("tbody").find_all("tr")[i].find("td").find("div").get_text()
                raw_members=raw_members.replace(",","")
                members=re.findall(r"\d+",raw_members)
        except:
            pass
        
    #従業員数補足情報
    for i in range(0,10):
        try:
            juge=soup.find("table",class_="jobOfferTable thL").find("tbody").find_all("tr")[i].find("th").get_text()
            if "従業員数" in juge:
                raw_members_etc=soup.find("table",class_="jobOfferTable thL").find("tbody").find_all("tr")[i].find("td").find("div").get_text()
                members_etc=raw_members_etc.split()
        except:
            pass
    
    #法人住所
    for i in range(0,10):
        try:
            juge=soup.find("table",class_="jobOfferTable thL").find("tbody").find_all("tr")[i].find("th").get_text()
            if "本社所在地" in juge:
                raw_houjin_addr=soup.find("table",class_="jobOfferTable thL").find("tbody").find_all("tr")[i].find("td").find("div").get_text()
                houjin_addr=raw_houjin_addr
        except:
            pass

    #郵便番号
    for i in range(10):
      try:
        juge=soup.find("table",class_="jobOfferTable thL").find("tbody").find_all("tr")[i].find("th").get_text()
        if "所在地" in juge:
            raw_postcode=soup.find("table",class_="jobOfferTable thL").find("tbody").find_all("tr")[i].find("td").find("div").get_text()
            raw_postcode=raw_postcode.replace("-","")
            raw_postcode=re.findall(r"\d+",raw_postcode)
            
            if len(raw_postcode[0]) == 7:
                postcode=raw_postcode
            else:
                pass
      except:
        pass 


    #職種
    for i in range(0,10):
        try:
            juge=soup.find("table",class_="jobOfferTable thL").find("tbody").find_all("tr")[i].find("th").get_text()
            if "事業内容" in juge:
                raw_job_type=soup.find("table",class_="jobOfferTable thL").find("tbody").find_all("tr")[i].find("td").find("div").get_text()
                job_type=raw_job_type
        except:
            pass

    new_list.append(media)
    new_list.append(houjin[0])
    new_list.append(postcode[0])
    new_list.append(houjin_addr)
    new_list.append(members[0])
    new_list.append(members_etc[0])
    new_list.append(Industry[0])
    new_list.append(job_type)
    new_list.append(employee[0])

    return new_list

url="https://tenshoku.mynavi.jp/list/?jobsearchType=14&searchType=18"
res=requests.get(url)
soup=BeautifulSoup(res.text,"html.parser")

selector="body > div.wrapper > div.container > div.result > div > p.result__num > em"
total_num=soup.select_one(selector).get_text()
total_num=int(total_num)

n=50
if total_num%n==0:
    total_page_num=total_num//n
else:
    total_page_num=total_num//n+1

url_list_001=list()
url="https://tenshoku.mynavi.jp/list/?jobsearchType=14&searchType=18"
url_list_001.append(url)

for i in (range(2,total_page_num+1)):
    url_key_001="https://tenshoku.mynavi.jp/list/pg"+str(i)+"/?jobsearchType=14&searchType=18"
    url_list_001.append(url_key_001)


url_list_002=list()

a=1
joblib_num=10

for n in tqdm(range(0,joblib_num)):
    try:
        resultList=joblib.Parallel(n_jobs=12,verbose=3)([joblib.delayed(joblib_get_url)(i) for i in range(n*a,(n+1)*a)])
        url_list_002.extend(resultList)
    except:
        pass

url_list_002_filtered=[x for x in url_list_002 if x is not None]
'''
for x in url_list_002:
  if x is not None
'''
flatten_url_list_002 = [ flatten for inner in url_list_002_filtered for flatten in inner ]

url_list_003=list()
for i in flatten_url_list_002:
    if i not in url_list_002:
        url_list_003.append(i)

b=1
joblib_num=100

all_list=list()

for n in tqdm(range(0,joblib_num)):
    try:
        resultList=joblib.Parallel(n_jobs=12,verbose=3)([joblib.delayed(joblib_get_data)(i) for i in range(n*b,(n+1)*b) ])
        all_list.extend(resultList)
    except:
        pass

all_list_filtered=[x for x in all_list if x[0] is not None]#法人名が取れたら表示（うまくいかない）




mynavi_df=pd.DataFrame(all_list_filtered,columns=["媒体目","法人名","郵便番号","法人住所","従業員数","従業員補足情報","業種","職種","雇用形態"])
mynavi_df.to_csv("mynavi_data_test_100.csv",encoding="cp932",index=False)

