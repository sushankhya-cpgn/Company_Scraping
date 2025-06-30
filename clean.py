import os
import pandas as pd
from sqlalchemy import create_engine

# ---------- CONFIG ----------
SOURCE_PATH = 'output'
TARGET_PATH = 'merged_csv'
OUTPUT_FILE = os.path.join(TARGET_PATH, 'merged_cleaned.csv')
MYSQL_URI = "mysql+mysqlconnector://root:rootpass@localhost:3306/mydb"
TABLE_NAME = "COMPANY_MASTER_BD"

# ---------- READ & MERGE CSV ----------
csv_files = [os.path.join(SOURCE_PATH, f) for f in os.listdir(SOURCE_PATH) if f.endswith('.csv')]

if not os.path.exists(TARGET_PATH):
    os.makedirs(TARGET_PATH)

if csv_files:
    df = pd.read_csv(csv_files[0])
    for file in csv_files[1:]:
        try:
            temp_df = pd.read_csv(file)
            df = pd.concat([df, temp_df], ignore_index=True)
        except Exception as e:
            print(f"Error reading {file}: {e}")
else:
    print(f"No CSV files found in {SOURCE_PATH}")
    df = pd.DataFrame()

# ---------- CLEAN ----------
def full_clean_address(df):
    str_cols = df.select_dtypes(include='object').columns
    for col in str_cols:
        df[col] = df[col].astype(str)
        df[col] = df[col].str.strip()
        df[col] = df[col].str.replace(u'\u00A0', '', regex=False)
        df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
        df[col] = df[col].str.replace(r',\s+', ', ', regex=True)
        df[col] = df[col].str.replace(r'\s+,', ',', regex=True)
    return df

if not df.empty:
    df = full_clean_address(df)
    if 'URL' in df.columns:
        df.drop(columns=['URL'], inplace=True)

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Cleaned CSV saved to: {OUTPUT_FILE}")

    # LOAD TO MYSQL 
    try:
        engine = create_engine(MYSQL_URI)
        df.to_sql(TABLE_NAME, con=engine, if_exists='replace', index=False)
        print(f"Data successfully loaded into MySQL table: {TABLE_NAME}")
    except Exception as e:
        print(f"Error writing to MySQL: {e}")
else:
    print("No data to clean or upload.")
