import pandas as pd
import numpy as np
from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator #type:ignore
# from airflow.operators.bash_operator import BashOperator #type: ignore
import os
# import overpy

def get_new_info(latitude, longitude, obj):
    import overpy
    # Initialize the Overpass API client
    api = overpy.Overpass()

    # Define the search radius in meters
    radius = 1000

    # Create an Overpass QL query to find schools within the radius of the given point
    query = """
    (
    node["amenity"="{obj}"](around:{radius},{lat},{lon});
    way["amenity"="{obj}"](around:{radius},{lat},{lon});
    relation["amenity"="{obj}"](around:{radius},{lat},{lon});
    );
    out center;
    """.format(radius=radius, lat=latitude, lon=longitude, obj=obj)

    # Execute the query
    result = api.query(query)

    # Extract and count the names of the objects
    count = result.nodes + result.ways + result.relations

    return len(count)

# Get the number of objects within 1 km of each house
def helper(df, obj):
    new_col = f"no_{obj}_1km"
    df[new_col] = df.apply(lambda row: get_new_info(row['latitude'], row['longitude'], obj), axis=1)
    print(f"Process {obj} done")

# Get the current date
def get_date():
    return datetime.now().date()

def add_surrounding_amenities(house_info, output_path):
    while True:
        try:
            if os.path.exists(house_info):
                df = pd.read_csv(output_path)
                break
            else:
                continue
        except:
            print("File not found")
    df = pd.read_csv(house_info)
    # df = pd.read_csv(f'./dags/data/house_info ({get_date()}).csv')
    # df = pd.read_csv(f'./dags/data/house_info (2024-04-07).csv')

    # Object list
    obj_arr = ['school', 'hospital', 'restaurant', 'cafe', 'bank', 'atm', 'marketplace', 'supermarket', 'fuel', 'pharmacy']

    # Execute the helper function for each object
    for obj in obj_arr:
        helper(df, obj)

    # df.to_csv(f'./dags/data/add_surrounding_amenities ({get_date()}).csv', index=False)
    df.to_csv(output_path, index=False)

if __name__ == "__main__":
    folder_path = os.path.join(os.path.dirname(__file__))
    dags_folder = os.path.dirname(folder_path)
    print(dags_folder)
    input_path = folder_path + f"/data/house_info ({get_date()}).csv"
    output_path = folder_path + f"/data/add_surrounding_amenities ({get_date()}).csv"
    # add_surrounding_amenities(input_path, output_path)
    