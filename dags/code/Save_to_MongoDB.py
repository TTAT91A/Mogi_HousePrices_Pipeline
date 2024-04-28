import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
import os
# from pre_processing import *

def import_csv_to_mongodb(df, database_name='House_prices', collection_name = "HCMCity", mongo_uri='mongodb+srv://nattan1811:taibitri123@cluster0.voqacs7.mongodb.net/'):
# mongodb+srv://nattan1811:<password>@cluster0.voqacs7.mongodb.net/
    # Read CSV into a Pandas DataFrame

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Convert DataFrame to dictionary for easier MongoDB insertion
    data = df.to_dict(orient='records')

    try:
        # Insert data into MongoDB
        collection.insert_many(data)
        print("Insert data to MongoDB successfully")
    except:
        print("Insert data to MongoDB failed")

    # Close MongoDB connection
    client.close()

def get_date():
    return datetime.now().date()

if __name__ == "__main__":
    # collection_name = "HCMCity"
    today = get_date()
    overpass_name = f'overpass({today}).csv'

    overpass_path = "https://raw.githubusercontent.com/TTAT91A/Mogi_HousePrices_Pipeline/main/dags/data1/" + overpass_name

    df = pd.read_csv(overpass_path)
    import_csv_to_mongodb(df)