# ExcelToSQL v1.0
# script to populate postgresql database with excel-files
# overwrites existing tables with same name
# make sure to enable slowly changing dimensions in data warehouse to keep history
# for questions contact mailto:jakob@fssthlm.se

from os import scandir
import pandas as pd
from sqlalchemy import create_engine

# postgresql connection string
conn_string = 'postgresql://postgres:pumperpw@localhost/OD_Dev'
db = create_engine(conn_string)
conn = db.connect()

# read files in directory
filelist = scandir(r'C:/Users/jakob/Documents/Python/ExcelToSQL_Indata/')

# create lists for valid, error and skipped files
valid_files = []
error_files = []
skipped_files = []

# filter out non-excel files and temp files
for file in filelist:
    if file.name.endswith('.xlsx') and not file.name.startswith('~'):
        valid_files.append(file.name)
    if file.name.startswith('~'):
        skipped_files.append(file.name)

print(f"Valid excel-files found: {valid_files}")
print('_' * 100)

# loop through files
for xl_file in valid_files:
    file_dir = f"C:/Users/jakob/Documents/Python/ExcelToSQL_Indata/{xl_file}"

    # create dataframe from excel-file
    df = pd.read_excel(file_dir)

    # add id and timestamp columns
    idval = df.index + 1
    df.insert(0, 'Id', idval)
    df['Timestamp'] = pd.Timestamp.now()

    # create table name from filename
    tbl_name = xl_file.replace('.xlsx', '')
    print(f"Populating/creating table {tbl_name} in database...")

    # populate table
    try:
        df.to_sql(tbl_name, con=conn, if_exists='replace', index=False)
        print(f"Table {tbl_name} populated/created successfully.", u'\u2713')
    except:
        print(f"Table {tbl_name} encountered an error while trying to populate it.")
        error_files.append(xl_file)

    print('_' * 100)

# close connection
conn.close()

# print results
if len(error_files) == 0:
    print(f"Script ended. No errors discovered.")
else:
    print(f"Script ended. Error discovered in files: {error_files}")
print('_'*100)
print(f'Files opened while script was running: {skipped_files}')

# move files to archive
if __name__ == "__main__":
    pass