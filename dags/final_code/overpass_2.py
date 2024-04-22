# -*- coding: utf-8 -*-
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import os

import overpy
import threading

def get_new_info(latitude, longitude, obj):
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

obj_arr = ['school', 'hospital', 'restaurant', 'cafe', 'bank', 'atm', 'marketplace', 'supermarket', 'fuel', 'pharmacy']

def helper(df, obj):
    new_col = f"no_{obj}_1km"
    df[new_col] = df.apply(lambda row: get_new_info(row['latitude'], row['longitude'], obj), axis=1)
    print(f"Process {obj} done")

# for obj in obj_arr:
#     helper(df, obj)

def overpass(house_info_processed, output_path):
    while True:
        try:
            if os.path.exists(house_info_processed):
                df = pd.read_csv(output_path)
                break
            else:
                continue
        except:
            print("File not found")
    df = pd.read_csv(house_info_processed)
    obj_arr = ['school', 'hospital', 'restaurant', 'cafe', 'bank', 'atm', 'marketplace', 'supermarket', 'fuel', 'pharmacy']

    p1 = threading.Thread(target=helper, args=(df, obj_arr[0]))

    p2 = threading.Thread(target=helper, args=(df, obj_arr[1]))

    p3 = threading.Thread(target=helper, args=(df, obj_arr[2]))

    p4 = threading.Thread(target=helper, args=(df, obj_arr[3]))

    p5 = threading.Thread(target=helper, args=(df, obj_arr[4]))

    p6 = threading.Thread(target=helper, args=(df, obj_arr[5]))

    p7 = threading.Thread(target=helper, args=(df, obj_arr[6]))

    p8 = threading.Thread(target=helper, args=(df, obj_arr[7]))

    p9 = threading.Thread(target=helper, args=(df, obj_arr[8]))

    p10 = threading.Thread(target=helper, args=(df, obj_arr[9]))

    p1.start()
    p2.start()

    # check if the thread is done
    p1.join()
    p2.join()

    p3.start()
    p4.start()

    # check if the thread is done
    p3.join()
    p4.join()

    p5.start()
    p6.start()

    # check if the thread is done
    p5.join()
    p6.join()

    p7.start()
    p8.start()

    # check if the thread is done
    p7.join()
    p8.join()

    p9.start()
    p10.start()

    # check if the thread is done
    p9.join()
    p10.join()


    df.to_csv(output_path, index=False)

def get_date():
    from datetime import datetime
    # return datetime.now().date()
    return "2024-04-18"

if __name__ == "__main__":
    folder_path = os.path.join(os.path.dirname(__file__))
    dags_folder = os.path.dirname(folder_path)
    print(dags_folder)
    input_path = folder_path + f"/data1/processed({get_date()}).csv"
    output_path = folder_path + f"/data1/({get_date()}).csv"
    