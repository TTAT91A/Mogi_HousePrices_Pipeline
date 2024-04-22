import numpy as np
import pandas as pd
from pathlib import Path 

def read_csv(text):
    df = pd.read_csv(text)
    return df
    
def duplicated(house_df):
    have_duplicate_rows = house_df.duplicated().any()
    if have_duplicate_rows == True:
        house_df.drop_duplicates(inplace=True)

def convert_to_number(text):
    total = 0
    if str(text) != 'nan':
        parts = text.split()
        if len(parts)==2:
            if parts[1]=='năm':
                total=float(parts[0])*365
            elif parts[1]=='tháng':
                total=float(parts[0])*30
            else:
                total=float(parts[0])
        elif len(parts)==4:
            if parts[1]=='năm':
                if parts[3]=='tháng':
                    total=float(parts[0])*365 + float(parts[2])*30
                else:
                    total=float(parts[0])*365 + float(parts[2])
            elif parts[1]=='tháng':
                total=float(parts[0])*30 + float(parts[2])
    else:
        return total
    return total 

def convert_data(house_df):
    #Tách giá trị diện tích ở cột area_used
    house_df['area_used']=house_df['area_used'].str.extract(r'(\d+)').astype(float)
    #Tách các giá trị diện tích, chiều dài, chiều rộng ở cột area
    extracted_data = house_df['area'].str.extract(r'(\d+(?:\.\d+)?) m2 \((\d+(?:,\d+)?)x(\d+(?:,\d+)?)\)')
    house_df['area'] = extracted_data[0].astype(float)
    house_df['witdh'] = extracted_data[1]
    house_df['length'] = extracted_data[2]
    house_df['witdh'] = house_df['witdh'].str.replace(',', '.')
    house_df['length'] = house_df['length'].str.replace(',', '.')
    house_df['witdh'] = house_df['witdh'].astype(float)
    house_df['length'] = house_df['length'].astype(float)
    #Tách giá trị cột seniority
    house_df['seniority'] = house_df['seniority'].apply( convert_to_number);

def missing_value(house_df):
    house_df['area_used'] = house_df['area_used'].fillna(house_df['area'])
    house_df=house_df.dropna();
    
def save_data(text):
    filepath = Path(text)  
    filepath.parent.mkdir(parents=True, exist_ok=True)  
    house_df.to_csv(filepath, encoding='utf-8-sig',index=False); 

import os
from datetime import date
if __name__ == '__main__':    
    folder_path = os.path.join(os.path.dirname(__file__))
    dags_path = os.path.dirname(folder_path)
    today = date.today()

    input_path = dags_path +f'/data1/house_info_today({today}).csv'
    output_path = dags_path +f'/data1/processed({today}).csv'

    house_df = read_csv(input_path)
    duplicated(house_df)
    convert_data(house_df) 
    missing_value(house_df)
    save_data(output_path)
