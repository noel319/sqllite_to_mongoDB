import os
import re
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient, InsertOne
from tqdm import tqdm
from dateutil.parser import parse
from detect_hugging_face import generate_name

# Remove Special charater from sqlite name
def sanitize_name(name):
    return re.sub(r'[^a-zA-Zа-яА-ЯіїІЇєЄ0-9_]', '_', name)

# Set data format
def format_date(value):
    date_patterns = [
        r'\b\w{3} \d{1,2}, \d{4}\b', #Matches sep 20, 2002
        r'\b\d{4}-\d{1,2}-\d{1,2}\b',  # Matches YYYY-MM-DD
        r'\b\d{1,2}\.\d{1,2}\.\d{4}\b',  # Matches DD.MM.YYYY
        r'\b\d{1,2}/\d{1,2}/\d{4}\b', #Matches DD/MM/YYYY
    ]
    combined_pattern = '|'.join(date_patterns)
    matches = re.findall(combined_pattern, value)    
    try:
        if matches:
            parse_date = parse(value)
            if parse_date.year and parse_date.month and parse_date.day:
                return parse_date.strftime('%d.%m.%Y')
            elif parse_date.year and parse_date.month:
                return f'01.{parse_date.strftime("%m.%Y")}'
            elif parse_date.year:
                return f'01.01.{parse_date.strftime("%Y")}'
        else:           
            return value        
    except(ValueError, TypeError, AttributeError):
        return value

column_name =[]

def format_and_rename_row(row, columns):
    formatted_row = {}
    for col, val in zip(columns, row):
        if isinstance(val, str):
            formatted_row[col] = format_date(val)
        else:
            formatted_row[col] = val
    return formatted_row

def migrate_table(engine, session, table, mongodb, chunk_size = 500):
    metadata = MetaData()
    table_reflected = Table(table.name, metadata, autoload_with=session.bind)
    query = f"SELECT * FROM {table.name} LIMIT 500"
    df = pd.read_sql(query, engine)
    column_data = {}
    for column in df.columns:
        column_data[column] = df[column].tolist()
        column_name.append(generate_name(column_data))
    print(column_name)
    query = session.query(table_reflected)
    mongo_collection = mongodb[table.name]
    # if table.name == "main_data" or table.name == "main_docsize":
    #     columns = table_reflected.columns.keys()
    # else:
    columns = column_name
    offset = 0

    while True:
        rows = query.offset(offset).limit(chunk_size).all()        
        if not rows:
            break
        documents = [format_and_rename_row(row, columns) for row in rows]
        mongo_collection.bulk_write([InsertOne(doc) for doc in documents])
        offset += chunk_size

def migrate_sqlite_to_mongo(sqlite_db_path, mongo_uri, chunk_size = 500):
    name = sanitize_name(os.path.basename(sqlite_db_path).replace('.db',''))    
    db_name = name.replace(' ', '_')
    if len(db_name) > 30:
        db_name = db_name[:30]

    engine = create_engine(sqlite_db_path)
    metadata = MetaData()
    metadata.reflect(bind=engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    mongo_client = MongoClient(mongo_uri)
    mongo_db = mongo_client[db_name]

    for table in metadata.sorted_tables:
        if table.name == "main_content" or table.name == "main_config":
            continue
        print(f"Migrateing table: {table.name} to MongoDB database: {db_name}")
        migrate_table(engine, session, table, mongo_db, chunk_size)
        print(f"Table {table.name} migrated successfully")
        column_name.clear()

    session.close()
    mongo_client.close()

def migrate_all(folder_path, mongo_uri, chunk_size =500):
    for filename in os.listdir(folder_path):
        if filename.endswith('.db'):
            sqlite_db_path = f"sqlite:///{os.path.join(folder_path, filename)}"
            print(f"Processing file : {filename}")
            migrate_sqlite_to_mongo(sqlite_db_path, mongo_uri, chunk_size)
            print(f"File {filename} migrated successfully.")


if __name__ == "__main__":
    folder_path = "testdb"
    mongo_uri = "mongodb://localhost:27017"
    chunk_size = 500
    migrate_all(folder_path, mongo_uri, chunk_size)

