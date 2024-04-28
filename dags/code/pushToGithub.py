import os
from datetime import datetime
from github import Github
import os
from dotenv import load_dotenv, dotenv_values

load_dotenv()
TOKEN = os.getenv('GITHUB_TOKEN')
today = datetime.now().date()


def connect_github(username, password, repo_name='House_Prices_Pipeline'):
    # g = Github(username, password)
    # repo = g.get_user().get_repo(repo_name)
    g = Github(password)
    repo = g.get_repo(username + "/" + repo_name)
    return repo

def get_all_files(username='TTAT91A', password=TOKEN, repo_name='Mogi_HousePrices_Pipeline'):
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

def pushToGithub(local_file_path, file_name, username='TTAT91A', password=TOKEN, repo_name='Mogi_HousePrices_Pipeline'):
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
    git_file = 'dags/data1/' + file_name #check file in repo
    if git_file in all_files:
        contents = repo.get_contents(git_file)
        commit = "Updated file " + str(today)
        repo.update_file(contents.path, commit, content, contents.sha, branch="main")
        print(git_file + ' UPDATED')
    else:
        commit = "Upload file " + str(today)
        repo.create_file(git_file, commit, content, branch="main")
        print(git_file + ' CREATED')