"""
版本v0说明：
此版本包含数据库改动的自备份功能；
在v1版本里，此功能被sql服务器的trigger脚本取代
本版本代码未完成

"""


import pandas as pd
import os

import sqlalchemy
from sqlalchemy import create_engine

# create database engine
engine = create_engine('postgresql://davis:zhangyi1212@localhost:5432/hami')  # connect to postgres server
print('Database opened successfully\n\n')


# sql 命令
# sql_cmd = 'SELECT * FROM hotel'

print('SQL query command example: SELECT hotel_name, 酒店名称 FROM hotel\n\n')
sql_cmd = input('pls input sql query command: ')
df = pd.read_sql(sql=sql_cmd, con=engine)


# output to excel


def to_excel(file_name):
    file_name = file_name + '.xlsx'
    writer = pd.ExcelWriter(file_name)
    df.to_excel(writer, 'Sheet1', index=False)
    writer.save()
    return file_name


output_file = to_excel('output')

output_loc = os.path.join(os.getcwd(), output_file)

print('Output done! The file is put into path %s' % output_loc)










