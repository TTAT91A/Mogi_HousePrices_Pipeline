from github import Github
import os

def push_to_github(filename, file_path):
    # token = Github('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')
    token = Github("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")
    repo = token.get_repo('TTAT91A/House_Prices_Pipeline')

    folder_path = os.path.join(os.path.dirname(__file__))
    data_folder_path = os.path.join(os.path.dirname(folder_path), "data")
    print(data_folder_path)
    with open(file_path, 'r', encoding='utf-8') as file:
        data = file.read()

    destination_git_path = 'dags/data1/' + filename
    commit = f"upload {filename}"

    repo.create_file(destination_git_path, commit, data, branch='main')

folder_path = os.path.join(os.path.dirname(__file__))
dags_folder = os.path.dirname(folder_path)

# filename = 'house_today(2024-04-18).csv'
# # 'house_today(2024-04-18).csv', 'file_path': './dags/data1/house_today(2024-04-18).csv'},

file_path = dags_folder + f'/data1/processed(2024-04-19).csv'
push_to_github('processed(2024-04-19).csv', './dags/data1/processed(2024-04-19).csv')


