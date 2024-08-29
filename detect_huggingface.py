import os
import requests
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
api_url = 'http://192.168.10.92:11434/api/generate' # Example using a GPT model; adjust as needed

def generate_name(column_data):    
    prompt = (
    f"here is my first 4 rows from table: {column_data}. Based on the given columns, try to deduce the possible column names. for example if following data in column: ‘Иван’, ‘Андрей’, ‘Алексей’, then return ‘first_name’ column"
    f"Do not include descriptions or any additional text, only the column names."
    f"Choose one name."
    f"I need only one name."
    )
    payload = {
        "model" : 'llama3.1:70b',
        "prompt": prompt,
        "stream" : False
        }

    response = requests.post(api_url, json=payload)

    if response.status_code == 200:
        result = response.json()
        generated_text = result['response']
        print(generated_text)    
        return generated_text
    else:
        print(f"Error: {response.status_code} - {response.text}")
engine = create_engine('sqlite:///testdb/ЗИЦ МВД-подучетные лица г. Москва (2001)6_2002_07_2024.db')
metadata = MetaData()
metadata.reflect(bind=engine)
for table in metadata.sorted_tables:
    print(f"Table name: {table.name}")
    query = f"SELECT * FROM {table.name} LIMIT 500"
    df = pd.read_sql(query, engine)
    column_data = {}
    for column in df.columns:
        column_data[column] = df[column].tolist()
        res =generate_name(column_data)
        print(f"columnname: {res}")
