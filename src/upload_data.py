import os
import pandas as pd
import sqlalchemy

# Define string connection
path_connection = 'sqlite:///{path}'

# Save path of main directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Find the data file names
files_names = os.listdir(DATA_DIR)

# Opening connection with database
path_connection = path_connection.format(path=os.path.join(DATA_DIR, 'olist.db'))
connection = sqlalchemy.create_engine(path_connection)

# Insert file into database
for file in files_names:
    df_tmp = pd.read_csv(os.path.join(DATA_DIR, file))
    table_name = "tb_" + file.strip(".csv").replace("olist_", "").replace("_dataset", "")
    df_tmp.to_sql(  table_name,
                    connection,
                    if_exists='replace',
                    index=False )