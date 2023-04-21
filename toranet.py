#https://toranet.jp/t/r/T100010s.jsp

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
  res=requests.get(url)
  soup=BeautifulSoup(res.text,"html.parser")
  for i in range(0,20):
    try:
      elem=soup.find_all("div",class_="jobItem lst_cst_wrap")[i].find("a")
      url_key=elem.attrs["href"]
      url_list_pre.append(url_key)
    except:
      pass
  return url_list_pre

def joblib_get_data(i):
    new_list=list()
    #法人名 支店名 法人住所 支店住所 従業員数 業種 職種 雇用形態
    postcode=media=members_etc=houjin=shiten=houjin_addr=shiten_addr=members=Industry=job_type=employee=" "

    url=url_list_003[i]
    res=requests.get(url)
    res.encoding = res.apparent_encoding
    soup=BeautifulSoup(res.text,"html.parser")

    #法人名
    for i in range(10):
        try:
            juge=soup.find("td",class_="fs-m fh-l",colspan="2").find("div").find_all("span")[i].get_text()
            if "会社名" in juge:
                raw_houjin=soup.find("td",class_="fs-m fh-l",colspan="2").find("div").find_all("span")[i].next_sibling.next_sibling.get_text()
                houjin=raw_houjin
        except:
            pass
    #従業員数
    for i in range(10):
        try:
            juge=soup.find("td",class_="fs-m fh-l",colspan="2").find("div").find_all("span")[i].get_text()
            if "従業員数" in juge:
                raw_members=soup.find("td",class_="fs-m fh-l",colspan="2").find("div").find_all("span")[i].next_sibling.next_sibling.get_text()
                raw_members=raw_members.replace(",","")
                members=re.findall(r"\d+",raw_members)                
        except:
            pass
    #職種
    for i in range(10):
        try:
            juge=soup.find("td",class_="fs-m fh-l",colspan="2").find("div").find_all("span")[i].get_text()
            if "事業内容" in juge:
                raw_job_type=soup.find("td",class_="fs-m fh-l",colspan="2").find("div").find_all("span")[i].next_sibling.next_sibling.get_text()
                job_type=raw_job_type.split()
        except:
            pass  
    #媒体名
    media="toranet"
    #法人住所
    for i in range(10):
        try:
            juge=soup.find("td",class_="fs-m fh-l",colspan="2").find("div").find_all("span")[i].get_text()
            if "所在住所" in juge:
                raw_houjin_addr=soup.find("td",class_="fs-m fh-l",colspan="2").find("div").find_all("span")[i].next_sibling.next_sibling.get_text()
                houjin_addr=raw_houjin_addr
        except:
            pass
      
    #郵便番号
    for i in range(10):
        try:
            juge=soup.find("td",class_="fs-m fh-l",colspan="2").find("div").find_all("span")[i].get_text()
            if "所在住所" in juge:
                raw_postcode=soup.find("td",class_="fs-m fh-l",colspan="2").find("div").find_all("span")[i].next_sibling.next_sibling.get_text()
                raw_postcode=raw_postcode.replace("-","")
                raw_postcode=re.findall(r"\d+",raw_postcode)
                if len(raw_postcode[0]) == 7:
                    postcode=raw_postcode
                else:
                    pass
        except:
            pass     
    #従業員補足情報
    for i in range(10):
        try:
            juge=soup.find("td",class_="fs-m fh-l",colspan="2").find("div").find_all("span")[i].get_text()
            if "従業員数" in juge:
                raw_members_etc=soup.find("td",class_="fs-m fh-l",colspan="2").find("div").find_all("span")[i].next_sibling.next_sibling.get_text()
                members_etc=raw_members_etc.split()
        except:
            pass    
    new_list.append(media)      
    new_list.append(houjin.replace("\n","").replace(" ",""))
    new_list.append(postcode[0])
    new_list.append(houjin_addr.replace("\n","").replace(" ",""))
    new_list.append(members[0])
    new_list.append(members_etc[0])
    new_list.append(job_type[0])
    
    return new_list


url="https://toranet.jp/t/r/T102010s.jsp"
res=requests.get(url)
soup=BeautifulSoup(res.text,"html.parser")

selector="#leftCol > div.list_paging > div.total > p > span"
total_num=soup.select_one(selector).get_text()
total_num=int(total_num)

if total_num%20==0:
    total_page_num=total_num//20
else:
    total_page_num=total_num//20+1

#print(total_num)
#なぜか493


url_list_001=list()
url_list_001.append(url)

for i in range(2,total_page_num):
    n=20*(i-1)
    url_key="https://toranet.jp/t/r/T102010s.jsp?index="+str(n)
    url_list_001.append(url_key)

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

all_list_filtered=[x for x in all_list if x[1] is not None]

toranet_df=pd.DataFrame(all_list_filtered,columns=["媒体名","法人名","郵便番号","法人住所","従業員数","従業員補足情報","職種"])
toranet_df.dropna(subset=["法人名"]).to_csv("tornet_data_test_100.csv",encoding="cp932",errors="ignore",index=False)


"""
#テスト用
#法人名
url=url_list_003[0]
res=requests.get(url)
res.encoding = res.apparent_encoding
soup=BeautifulSoup(res.text,'html.parser')
print(url)

houjin=" "
for i in range(10):
    try:
        juge=soup.find("td",class_="fs-m fh-l",colspan="2").find("div").find_all("span")[i].get_text()
        if "会社名" in juge:
            houjin=soup.find("td",class_="fs-m fh-l",colspan="2").find("div").find_all("span")[i].next_sibling.next_sibling.get_text()
    except:
        pass

print(houjin)
"""

