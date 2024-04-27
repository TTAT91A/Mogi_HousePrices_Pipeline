import urllib.parse
import pandas as pd
import requests
import numpy as np
# from selenium_provider import Selenium
# from selenium_provider import get_into
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
import time

import pushToGithub


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

def get_house_info(house_path, house_info):
    # os.path.isfile(house_path)
    df = pd.read_csv(house_path)

    dic = {'Diện tích sử dụng': 'area_used', 'Diện tích đất': 'area', 'Phòng ngủ': 'bedroom',
        'Nhà tắm': 'wc', 'Pháp lý': 'juridical', 'Ngày đăng': 'date_submitted', 'Mã BĐS': 'id'}
    dic_df = {'area_used': '', 'area': '', 'bedroom': '',
            'wc': '', 'juridical': '', 'date_submitted': '', 'id': ''}
    df_info = pd.DataFrame(columns=['address', 'latitude', 'longitude', 'describe', 'area_used',
                        'area', 'bedroom', 'wc', 'juridical', 'date_submitted', 'id', "seller", "seniority", "phone"])

    # Chưa parse seller
    cc = 0
    for i in df['link']:
        dic_df = {'area_used': '', 'area': '', 'bedroom': '',
                'wc': '', 'juridical': '', 'date_submitted': '', 'id': ''}

        time.sleep(2)
        try:
            response = requests.get(i, headers= headers, timeout=60)
            print(i, response.status_code)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                try:
                    address = soup.find('div', class_='address').text
                except:
                    address = ''
                info = soup.find_all('div', class_='info-attr clearfix')
                for i in info:
                    tmp = i.text.strip().split('\n')
                    dic_df[dic[tmp[0]]] = tmp[1]
                try:
                    describe = soup.find('div', class_='info-content-body').text
                except:
                    describe = ''
                try:
                    lat_long = soup.find('div', class_='map-content clearfix').find(
                        'iframe').get('data-src').split('=')[-1].split(',')
                    latitude = lat_long[0]
                    longitude = lat_long[1]
                except:
                    latitude, longitude = '', ''
                try:
                    seller = soup.find('div', class_='agent-name').text.strip('\n')
                except:
                    seller = ''
                try:
                    seniority = soup.find(
                        'div', class_='agent-date').text.replace('Đã tham gia: ', '')
                except:
                    seniority = ''
                try:
                    phone = soup.find(
                        'div', class_='agent-contact clearfix').find('span').text.strip(' ')
                except:
                    phone = ''
                # link_seller = 'https://mogi.vn' + soup.find('div', class_ = 'agent-name').find('a').get('href')

                arr = [address, latitude, longitude, describe]
                for ii in dic_df.keys():
                    arr.append(dic_df[ii])
                # arr.extend([seller, seniority, phone, link_seller])
                arr.extend([seller, seniority, phone])

                new_row = pd.Series(arr, index=df_info.columns)
                df_info.loc[len(df_info)] = new_row

                cc += 1
                # if cc == 10: break
            else:
                print("Timeout: ",i)
        except:
            print("Timeout: ",i)

        

    df_info.to_csv(house_info, index=None)

def get_date():
    return datetime.now().date()

if __name__ == "__main__":
    folder_path = os.path.join(os.path.dirname(__file__))
    dags_folder = os.path.dirname(folder_path)
    # input_path = dags_folder + f"/data/house ({today}).csv"
    # output_path = dags_folder + f"/data/house_info ({today}).csv"

    all_files_github = pushToGithub.get_all_files(repo_name='Mogi_HousePrices_Pipeline')
    file_house_name = f'house_today({get_date()}).csv'
    house_path = "dags/data1/" + file_house_name

    #check house_path in all_files_github
    if house_path in all_files_github:
        house_info_name = f'house_info_today({get_date()}).csv'

        input_path = "https://raw.githubusercontent.com/TTAT91A/Mogi_HousePrices_Pipeline/main/dags/data1/" + file_house_name

        output_path = dags_folder + "/data1/" + house_info_name        
        # export house_info file to local
        get_house_info(input_path, output_path)

        # push house_info file to github
        pushToGithub.pushToGithub(local_file_path=output_path, file_name=house_info_name ,repo_name='Mogi_HousePrices_Pipeline')
    else:
        print(f"{house_path} not found")