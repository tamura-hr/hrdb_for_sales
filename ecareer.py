from bs4 import BeautifulSoup
import requests
import joblib
from tqdm import tqdm
import pandas as pd

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


