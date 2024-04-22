import urllib.parse
import pandas as pd
import requests
import numpy as np
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
from github import Github
from github import Auth
import os
from dotenv import load_dotenv, dotenv_values

load_dotenv()
TOKEN = os.getenv('GITHUB_TOKEN')

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
today = datetime.now().date()
FILE_NAME = f'house({today}).csv'


def get_house_link(path):
    df = pd.DataFrame(columns=['name', 'district', 'price', 'bedroom', 'wc', 'acreage', 'link', 'date'])
    thu = 0
    i = 1
    while True:
        link_root = 'https://mogi.vn/ho-chi-minh/mua-nha?cp='+str(i)
        ##
        while True:
            try:
                response = requests.get(link_root, headers= headers, timeout=20)
                if response.status_code == 200:
                    break
            except:
                continue  
        # response = requests.get(link_root, timeout=20, headers=headers)
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
        i += 1

    df.to_csv(path,index=None)

def get_date():
    return datetime.now().date()

def connect_github(username, password, repo_name='House_Prices_Pipeline'):
    g = Github(username, password)
    repo = g.get_user().get_repo(repo_name)
    return repo

def get_all_files(username='TTAT91A', password=TOKEN, repo_name='House_Prices_Pipeline'):
    # repo = g.get_user().get_repo('Mogi_HousePrices_Pipeline')
    repo = connect_github(username, password, repo_name=repo_name)

    all_files = []
    contents = repo.get_contents("")
    while contents:
        file_content = contents.pop(0)
        if file_content.type == "dir":
            contents.extend(repo.get_contents(file_content.path))
        else:
            file = file_content
            all_files.append(str(file).replace('ContentFile(path="','').replace('")',''))
    return all_files

def pushToGithub(local_file_path, username='TTAT91A', password=TOKEN, file_name=FILE_NAME, repo_name='House_Prices_Pipeline'):
    # g = Github(username, password)
    # # repo = g.get_user().get_repo('Mogi_HousePrices_Pipeline')
    # repo = g.get_user().get_repo('House_Prices_Pipeline')

    repo = connect_github(username, password, repo_name=repo_name)
    all_files = get_all_files(username, password, repo_name=repo_name)

    if os.path.exists(local_file_path):
        with open(local_file_path, 'r', encoding='utf-8') as file:
            content = file.read()
    else:
        print(f'{local_file_path} not found')
        return

    # Upload to github
    git_file = 'dags/data/' + file_name #check file in repo
    if git_file in all_files:
        contents = repo.get_contents(git_file)
        commit = "Upload file " + str(today)
        repo.update_file(contents.path, commit, content, contents.sha, branch="main")
        print(git_file + ' UPDATED')
    else:
        commit = "Upload file " + str(today)
        repo.create_file(git_file, commit, content, branch="main")
        print(git_file + ' CREATED')

if __name__ == '__main__':
    folder_path = os.path.join(os.path.dirname(__file__))
    dags_folder = os.path.dirname(folder_path)
    today = str(get_date())

    file_name = FILE_NAME
    dest_path = dags_folder + f"/data/" + file_name
    # get_house_link(dest_path) #write to file
    # pushToGithub(local_file_path=dest_path, file_name=file_name, repo_name='Mogi_HousePrices_Pipeline')
    print(get_all_files(repo_name="Mogi_HousePrices_Pipeline"))
