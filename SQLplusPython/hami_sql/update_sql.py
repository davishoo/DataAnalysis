import pandas as pd
import os
import re

import sqlalchemy
from sqlalchemy import create_engine
import psycopg2

con = psycopg2.connect(database="hami", user="davis", password="zhangyi1212", host="127.0.0.1", port="5432")
print("Database opened successfully!\n\n")
cur = con.cursor()


# Input data
def source_path():
    # import os
    print('Pls specify the path of input file ...')
    print('Example: D:\\file\\python\\AutomatePDF\n')
    path = input()  # call for input
    source_folder = os.path.abspath(path)  # make sure the path is absolute one
    return source_folder


sourceFile = 'update_hotel.xlsx'
absPath = os.path.join(source_path(), sourceFile)
inputObj = pd.ExcelFile(absPath)  # specify input data source
df = pd.read_excel(inputObj, 'Sheet1')

# get column names for update
pattern = r'[(),:;%/.+–-]'  # gonna remove unnecessary characters
regex = re.compile(pattern)

col_list_raw = list(df.columns)
col_list = []
for col_name in col_list_raw:
    cln_col_name = regex.sub('', col_name)    # 将列名进行处理，去掉多余的符号
    cln_col_name = cln_col_name.strip()       # 去掉左右空格
    col_list.append(cln_col_name)

col_list.remove('hotel_id')
print('\nWill update designated columns: \n %s' % col_list)

# print('Example: 客房总数')
# input_col = input('Pls specify the columns you want to update: \n')       # 接收用户输入

count_value = 0
for update_col in col_list:
    for index, row in df.iterrows():
        sql = 'UPDATE hotel_test SET "{}" = %s WHERE hotel_id = %s'.format(update_col)
        cur.execute(sql, (row[update_col], row['hotel_id']))
        count_value += 1
con.commit()
con.close()
print('\n\nDone! Total updated values: %s\n\n' % count_value)

# above code works





