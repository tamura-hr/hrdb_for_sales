#https://next.rikunabi.com/
#https://next.rikunabi.com/rnc/docs/cp_s00700.jsp?leadtc=top_kvs_submit

from bs4 import BeautifulSoup
import requests
import joblib
from tqdm import tqdm
import pandas as pd
import re

def joblib_get_url(i):
      #iにはページ数
  url_list_pre=list()
  url=url_list_001[i]
  raw_url="https://next.rikunabi.com"

  res=requests.get(url)
  soup=BeautifulSoup(res.text,"html.parser")

  for i in range(0,50):
    try:
      elem=soup.find_all("li",class_="rnn-jobOfferList__item rnn-group rnn-group--s js-kininaruItem")[i].find("div").find("h2").find("a")

      
      url_key=elem.attrs["href"]
      url_key=raw_url+url_key
      url_list_pre.append(url_key)
    except:
      pass
  return url_list_pre

def joblib_get_url_second(i):
  url_list_pre_second=list()
  url=url_list_003[i]

  res=requests.get(url)
  soup=BeautifulSoup(res.text,"html5lib")
  raw_url_second="https://next.rikunabi.com"
  try:
    elem_second=soup.find("div",class_="rn3-companyOfferTabMenu rn3-stage").find("div").find("ul").find_all("li")[1].find("a")
    url_key_second=elem_second.attrs["href"]
    url_key_second=raw_url_second+url_key_second
    url_list_pre_second.append(url_key_second)
  except:
     pass
  return url_list_pre_second

def joblib_get_data(i):
  new_list=list()
  #法人名 支店名 法人住所 支店住所 従業員数 業種 職種 雇用形態
  postcode=media=members_etc=houjin=members=houjin_addr=members=Industry=job_type=" "

  url=url_list_005[i]
  res=requests.get(url)
  res.encoding = res.apparent_encoding
  soup=BeautifulSoup(res.content,"html.parser")

  #媒体名
  media="rikunabi"

  #法人名
  for i in range(10):
    try:
        juge=soup.find("div",class_="rn3-companyOfferCompany").find_all("div",class_="rn3-companyOfferCompany__info")[i].find("h3").get_text()
        if "社名" in juge:
           raw_houjin=soup.find("div",class_="rn3-companyOfferCompany").find_all("div",class_="rn3-companyOfferCompany__info")[i].find("p").find("a").get_text()
           houjin=raw_houjin.split()
    except:
       pass
  
  #従業員数
    for i in range(10):
      try:
        judge=soup.find("div",class_="rn3-companyOfferCompany").find_all("div",class_="rn3-companyOfferCompany__info")[i].find("h3").get_text()
        if "従業員数" in judge:
            raw_members=soup.find("div",class_="rn3-companyOfferCompany").find_all("div",class_="rn3-companyOfferCompany__info")[i].find("p").get_text()
            raw_members=raw_members.replace(",","")
            members=re.findall(r"\d+",raw_members)
      except:
        pass

  
  #従業員補足情報
  for i in range(10):    
    try:
        juge=soup.find("div",class_="rn3-companyOfferCompany").find_all("div",class_="rn3-companyOfferCompany__info")[i].find("h3").get_text()
        if "従業員数" in juge:
           raw_members_etc=soup.find("div",class_="rn3-companyOfferCompany").find_all("div",class_="rn3-companyOfferCompany__info")[i].find("p").get_text()
           members_etc=raw_members_etc.split()
    except:
       pass 
  #郵便番号
  for i in range(10):    
    try:
        juge=soup.find("div",class_="rn3-companyOfferCompany").find_all("div",class_="rn3-companyOfferCompany__info")[i].find("h3").get_text()
        if "事業所" in juge:
           raw_postcode=soup.find("div",class_="rn3-companyOfferCompany").find_all("div",class_="rn3-companyOfferCompany__info")[i].find("p").get_text()
           raw_postcode=raw_postcode.replace("-","")
           raw_postcode=re.findall(r"\d+",raw_postcode)
            
           if len(raw_postcode[0]) == 7:
                postcode=raw_postcode
           else:
              pass
    except:
       pass  
      
  #法人住所
  for i in range(10):    
    try:
        juge=soup.find("div",class_="rn3-companyOfferCompany").find_all("div",class_="rn3-companyOfferCompany__info")[i].find("h3").get_text()
        if "事業所" in juge:
           raw_houjin_addr=soup.find("div",class_="rn3-companyOfferCompany").find_all("div",class_="rn3-companyOfferCompany__info")[i].find("p").get_text()
           houjin_addr=raw_houjin_addr
    except:
       pass     
  #業種   
  for i in range(10):    
    try:
        juge=soup.find("div",class_="rn3-companyOfferCompany").find_all("div",class_="rn3-companyOfferCompany__info")[i].find("h3").get_text()
        if "業種" in juge:
           raw_Industry=soup.find("div",class_="rn3-companyOfferCompany").find_all("div",class_="rn3-companyOfferCompany__info")[i].find("p").get_text()
           Industry=raw_Industry.split()
    except:
       pass
  #職種
  for i in range(10):    
    try:
        juge=soup.find("div",class_="rn3-companyOfferCompany").find_all("div",class_="rn3-companyOfferCompany__info")[i].find("h3").get_text()
        if "事業内容" in juge:
           raw_job_type=soup.find("div",class_="rn3-companyOfferCompany").find_all("div",class_="rn3-companyOfferCompany__info")[i].find("p").get_text()
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

  return new_list

  
url="https://next.rikunabi.com/lst/"
res=requests.get(url)
soup=BeautifulSoup(res.text,"html.parser")

selector="#searchForm > div > div.rn3-ActionSearch.js-actionSearch > div.rn3-ActionSearch__action > button > span.js-searchCountResult"
total_num=soup.select_one(selector).get_text()
total_num=int(total_num)

if total_num%50==0:
    total_page_num=total_num//50
else:
    total_page_num=total_num//50+1

url_list_001=list()
url_list_001.append(url)

for i in range(2,101):
    n=(i-1)*50+1
    url_key="https://next.rikunabi.com/lst/crn"+str(n)+".html"

    url_list_001.append(url_key)


url_list_002 = list()
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

url_list_004 = list()
'''
a=100
'''
a=1#テスト用

'''
for n in tqdm(range(len(url_list_003))):
'''
for n in tqdm(range(100)):#テスト用
    try:
        resultList = joblib.Parallel(n_jobs=12, verbose=3)( [joblib.delayed(joblib_get_url_second)(i) for i in range(n*a,(n+1)*a) ])
        url_list_004.extend(resultList)
    except:
        pass

url_list_004_filtered=[x for x in url_list_004 if x is not None]
'''
for x in url_list_004:
  if x is not None
'''
flatten_url_list_004 = [ flatten for inner in url_list_004_filtered for flatten in inner ]


url_list_005 = list()
for i in flatten_url_list_004:
  if i not in url_list_004:
      url_list_005.append(i)


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


rikunabi_df=pd.DataFrame(all_list_filtered,columns=["媒体名","法人名","郵便番号","法人住所","従業員数","従業員補足情報","業種","職種"])

rikunabi_df.to_csv("rikunabi_data_test_100.csv",encoding="utf-8-sig",index=False)

"""
#テスト用
#法人名
url=url_list_005[0]
print(url)
res=requests.get(url)
res.encoding = res.apparent_encoding
soup=BeautifulSoup(res.text,'html.parser')
houjin=" "
for i in range(10):
    try:
        juge=soup.find("div",class_="rn3-companyOfferContent__inner").find("div",class_="rn3-companyOfferContent__section js-cmpnyInfo").find("div",class_="rn3-companyOfferCompany").find_all("div",class_="rn3-companyOfferCompany__info")[i].find("h3").get_text()
        if "社名" in juge:
            houjin=soup.find("div",class_="rn3-companyOfferContent__inner").find("div",class_="rn3-companyOfferContent__section js-cmpnyInfo").find("div",class_="rn3-companyOfferCompany").find_all("div",class_="rn3-companyOfferCompany__info")[i].find("p").find("a").get_text()
    except:
        pass

print(houjin)

"""




