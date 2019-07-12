import pandas as pd
import os

import sqlalchemy
from sqlalchemy import create_engine

# input data and transform data to DataFrame


def source_path():
    # import os
    print('Pls specify the path of input file ...')
    print('Example: D:\\file\\python\\AutomatePDF\n\n')
    path = input()  # call for input
    source_folder = os.path.abspath(path)  # make sure the path is absolute one
    return source_folder


sourceFile = 'hotelData.xlsx'
absPath = os.path.join(source_path(), sourceFile)

inputObj = pd.ExcelFile(absPath)  # specify input data source
hotelDF = pd.read_excel(inputObj, 'Sheet1')

hotelDF.rename(columns={'酒店英文名称': 'hotel_name', '酒店中文名称': '酒店名称'}, inplace=True)


def remove_duplicates(data):     # argument 'data' is DataFrame type
    data.drop_duplicates(['hotel_name'], keep='first', inplace=True)
    data.drop_duplicates(['酒店名称'], keep='first', inplace=True)
    data.drop_duplicates(['详细地址'], keep='first', inplace=True)
    return data


txt_column_list = [
            'hotel_name', '酒店名称', '城市', '酒店位置',
            '详细地址', '前台电话', '销售姓名', '销售手机',
            '销售座机', '销售微信', 'QQ', '销售邮箱']


def remove_whitespace(df, col_list):   # 去掉左右空格
    for txt_col in col_list:
        df[txt_col] = df[txt_col].str.strip()


hotelDF_cleaned = remove_duplicates(hotelDF)
remove_whitespace(hotelDF_cleaned, txt_column_list)


hotel_id = []

for i in range(len(list(hotelDF_cleaned.index))):
    hotel_id.append('HK-' + str(i+1).zfill(4))

hotelDF_cleaned['hotel_id'] = hotel_id

hotel_metaData = hotelDF_cleaned[[
                                'hotel_id', 'hotel_name', '酒店名称',
                                '城市', '酒店位置', '详细地址', '客房总数']]

hotel_contacts = hotelDF_cleaned[[
                                'hotel_id', 'hotel_name', '酒店名称', '前台电话', '销售姓名',
                                '销售手机', '销售座机',
                                '销售微信', 'QQ', '销售邮箱']]

# above codes work

# write DataFrame into sql
engine = create_engine('postgresql://davis:zhangyi1212@localhost:5432/hami')  # connect to postgres server
print('Database opened successfully')

hotel_contacts.to_sql('hotel_contacts', con=engine, schema='public', index=False, if_exists='replace')
hotel_metaData.to_sql('hotel', con=engine, schema='public', index=False, if_exists='replace')










