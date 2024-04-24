import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
import os
# from pre_processing import *

def import_csv_to_mongodb(df, collection_name, database_name='House_prices', mongo_uri='mongodb+srv://nattan1811:taibitri123@cluster0.voqacs7.mongodb.net/'):
# mongodb+srv://nattan1811:<password>@cluster0.voqacs7.mongodb.net/
    # Read CSV into a Pandas DataFrame

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Convert DataFrame to dictionary for easier MongoDB insertion
    data = df.to_dict(orient='records')

    # Insert data into MongoDB
    collection.insert_many(data)

    # Close MongoDB connection
    client.close()

def get_date():
    return datetime.now().date()

if __name__ == "__main__":
    
    collection_name = "HCMCity"

    # df = pd.read_csv(f"dags/data/house_info ({get_date()}).csv")
    # import_csv_to_mongodb(df, collection_name)
    folder_path = os.path.join(os.path.dirname(__file__))
    dags_folder = os.path.dirname(folder_path)
    df = pd.read_csv(f"{dags_folder}/dags/data/house_info({get_date()}).csv")

    # df = pd.read_csv("../data/house_info (2022-04-16).csv")
    # df = pd.read_csv(f"{dags_folder}/data/house_info (2022-04-16).csv")
    print(df)