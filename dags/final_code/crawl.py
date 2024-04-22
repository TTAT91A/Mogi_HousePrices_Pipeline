import urllib.parse
import pandas as pd
import requests
import math
import time
import numpy as np
import requests
from bs4 import BeautifulSoup
from datetime import date
from datetime import datetime
import os

headers = {
  "Accept": "*/*",
  "Accept-Encoding": "gzip, deflate, br",
  "Accept-Language": "vi,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
  "Referer": "https://shopee.vn/",
  "Sec-Ch-Ua": "\"Microsoft Edge\";v=\"119\", \"Chromium\";v=\"119\", \"Not?A_Brand\";v=\"24\"",
  "Sec-Ch-Ua-Mobile": "?0",
  "Sec-Ch-Ua-Platform": "\"Windows\"",
  "Sec-Fetch-Dest": "empty",
  "Sec-Fetch-Mode": "cors",
  "Sec-Fetch-Site": "same-origin",
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0"
}

max_page = 2000
today = date.today()

"""#### Cào nhà."""
def get_house_link(path):
    df = pd.DataFrame(columns=['name', 'district', 'price', 'bedroom', 'wc', 'acreage', 'link', 'date'])
    thu = 0
    for i in range(1, max_page):
        link_root = 'https://mogi.vn/ho-chi-minh/mua-nha?cp='+str(i)
        ##
        response = requests.get(link_root)
        soup = BeautifulSoup(response.content, 'html.parser')

        content0 = soup.find('ul', class_ = 'props').find_all('a', class_ = 'link-overlay')
        content1 = soup.find_all('div', class_ = 'prop-addr')
        content2 = soup.find_all('div', class_ = 'price')
        content3 = soup.find_all('ul', class_ = 'prop-attr')
        content4 = soup.find_all('h2', class_ = 'prop-title')
        content5 = soup.find_all('div', class_ ='prop-created')

        for x, y, z, t, u, v in zip(content0, content1, content2, content3, content4, content5):
            link = x.get('href')
            district = y.text
            price = z.text
            tmp = t.text.strip().split('\n')
            bedroom, wc, acreage = tmp[1], tmp[2], tmp[0]
            name = u.text
            day = v.text
            if day != 'Hôm nay':
                thu = 1
                break

            new_row = pd.Series([name, district, price, bedroom, wc, acreage, link, today], index=df.columns)
            df.loc[len(df)] = new_row

        if thu == 1: break
        print('page:', i)

    df.to_csv(path,index=None)

"""#### Cào thông tìn chi tiết của từng nhà."""

def get_house_info(house_path, house_info):
    df = pd.read_csv(house_path)

    dic = {'Diện tích sử dụng': 'area_used', 'Diện tích đất': 'area', 'Phòng ngủ': 'bedroom', 'Nhà tắm': 'wc', 'Pháp lý': 'juridical', 'Ngày đăng': 'date_submitted', 'Mã BĐS': 'id'}
    dic_df = {'id': '', 'area_used':'', 'area': '', 'bedroom': '', 'wc': '', 'juridical': '', 'date_submitted': ''}
    df_info = pd.DataFrame(columns=['id', 'area_used', 'area', 'bedroom', 'wc', 'juridical', 'date_submitted', 'link', 'address', 'latitude', 'longitude', 'describe', "seller", "seniority", "phone", "link_seller"])

    cc = 0

    for link in df['link']:
        dic_df = {'id': '', 'area_used':'', 'area': '', 'bedroom': '', 'wc': '', 'juridical': '', 'date_submitted': ''}
        # response = requests.get(link)
        try:
            response = requests.get(link, headers= headers, timeout=10)
            print("Success")
        except:
            print("Timeout")
        soup = BeautifulSoup(response.content, 'html.parser')

        try: address = soup.find('div', class_ = 'address').text
        except: address = ''
        info = soup.find_all('div', class_ = 'info-attr clearfix')
        for i in info:
            tmp = i.text.strip().split('\n')
            dic_df[dic[tmp[0]]] = tmp[1]
        try: describe = soup.find('div', class_ = 'info-content-body').text
        except: describe = ''
        try:
            lat_long = soup.find('div', class_ = 'map-content clearfix').find('iframe').get('data-src').split('=')[-1].split(',')
            latitude = lat_long[0]
            longitude = lat_long[1]
        except: latitude, longitude = '', ''
        try: seller_name = soup.find('div', class_ = 'agent-name').text.replace('\n', '').replace('\r', '').strip()
        except: seller_name = ''
        try: seniority = soup.find('div', class_ = 'agent-date').text.replace('Đã tham gia: ', '')
        except: seniority = ''
        try: phone = soup.find('div', class_ = 'agent-contact clearfix').find('span').text.strip(' ')
        except: phone = ''
        try:
            seller = soup.find('div', class_ = 'agent-name').find('a').get('href')
            link_seller = 'https://mogi.vn' + seller
        except:
            link_seller = ''

        arr = []
        for ii in dic_df.keys(): arr.append(dic_df[ii])
        arr.extend([link, address, latitude, longitude, describe])
        arr.extend([seller_name, seniority, phone, link_seller])

        new_row = pd.Series(arr, index=df_info.columns)
        df_info.loc[len(df_info)] = new_row

        cc += 1
        print('link:',cc)

        # if cc == 10: break

    df_info.to_csv(house_info,index=None)

def get_date():
    # return date.today()
    return datetime.now().date()


if __name__ == "__main__":
    folder_path = os.path.join(os.path.dirname(__file__))
    dags_folder = os.path.dirname(folder_path)
    today = str(get_date())
    input_path = dags_folder + f"/data/house_today({today}).csv"
    output_path = dags_folder + f"/data/house_info_today({today}).csv"

    print(input_path)
    print(output_path)
    get_house_link(input_path)
    time.sleep(1)
    while True:
        if os.path.isfile(input_path):
            break
        else:
            continue
    get_house_info(input_path, output_path)
    
