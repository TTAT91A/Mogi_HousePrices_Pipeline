import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
import os

today = datetime.now().date()
def connect_mongodb(database_name='House_prices', collection_name = "HCMCity", mongo_uri='mongodb+srv://nattan1811:taibitri123@cluster0.voqacs7.mongodb.net/'):
    client = MongoClient(mongo_uri)
    db = client[database_name]
    col = db[collection_name]
    return col

# Extract all data
def extract_mongodb(database_name='House_prices', collection_name = "HCMCity", mongo_uri='mongodb+srv://nattan1811:taibitri123@cluster0.voqacs7.mongodb.net/'):
    col = connect_mongodb(database_name, collection_name, mongo_uri)
    x = col.find({})
    return pd.DataFrame(list(x))

# Extract by date
def extract_by_date(start_date:str=today, end_date:str=None, database_name='House_prices', collection_name = "HCMCity", mongo_uri='mongodb+srv://nattan1811:taibitri123@cluster0.voqacs7.mongodb.net/'):
    st_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    ed_date = datetime.strptime(end_date, '%Y-%m-%d').date() if end_date else st_date

    col = connect_mongodb(database_name, collection_name, mongo_uri)
    
    date_conditions = [str((st_date + timedelta(days=i)).strftime('%d/%m/%Y')) for i in range((ed_date - st_date).days + 1)]    

    query = {
        'date_submitted': {'$in': date_conditions}
    }
    
    x = col.find(query)
    return pd.DataFrame(list(x))

if __name__ == "__main__":
    # db = extract_mongodb()
    # print(len(db))
    # print(today)
    # start_date = '2024-05-01'
    # st_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    # print(str(st_date.strftime('%d/%m/%Y')))
    print(extract_by_date('2024-05-01', '2024-05-03'))