import pandas as pd
import os

import sqlalchemy
from sqlalchemy import create_engine

engine = create_engine('postgresql://davis:zhangyi@localhost:5432/postgres')


def sourcePath():
    # import os
    print('Pls specify the path of input file ...')
    print('Example: D:\\file\\python\\AutomatePDF\n\n')
    path = input()  # call for input
    sourceFolder = os.path.abspath(path)  # make sure the path is absolute one
    return sourceFolder


sourceFile = 'hotel_to_sql.xlsx'
absPath = os.path.join(sourcePath(), sourceFile)

inputObj = pd.ExcelFile(absPath)  # specify input data source
hotelDF = pd.read_excel(inputObj, 'Sheet1')

hotelDF.columns = ['HotelName', '酒店名称', '城市', '酒店位置', '详细地址', '客房总数']  # define column name of level_1

# prepare to write dataframe into sql
engine = create_engine('postgresql://davis:zhangyi1212@localhost:5432/hami')  # connect to postgres server
print('Database opened successfully')

hotelDF.to_sql('hotel', con=engine, index=False, if_exists='append')