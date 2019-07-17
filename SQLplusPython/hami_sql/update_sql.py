import pandas as pd
import os
import re
# import sqlalchemy
from sqlalchemy import create_engine
import psycopg2


def psycopg2_cur():  # 创建psycopg2游标对象
    conn = psycopg2.connect(database="hami", user="davis", password="zhangyi1212", host="127.0.0.1", port="5432")
    return conn.cursor(), conn


def sqlalchemy_engine():   # 创建sqlalchemy引擎
    return create_engine('postgresql://davis:zhangyi1212@localhost:5432/hami')


cur, con = psycopg2_cur()
engine = sqlalchemy_engine()
print("Database opened successfully!\n\n")


def full_input_path(file_name):   # 询问输入文件路径，然后返回带完整路径的输入文件名
    print('Pls specify the path of input file ...')
    print('Example: D:\\file\\python\\AutomatePDF\n')
    path = input()  # call for input
    source_folder = os.path.abspath(path)  # make sure the path is absolute one
    abs_path = os.path.join(source_folder, file_name)
    return abs_path


def get_input_df(source):   # 将输入excel数据转换为DataFrame
    input_file = full_input_path(source)      # 将输入文件的完整路径赋值给input_file
    input_obj = pd.ExcelFile(input_file)  # 创建EXCEL目标对象
    df_local = pd.read_excel(input_obj, 'Sheet1')   # 读取目标对象，返回DataFrame
    return df_local


sourceFile = 'update_hotel.xlsx'  # 定义输入数据的文件名
df = get_input_df(sourceFile)   # 得到DataFrame
print('Transform input data to DataFrame successfully!\n\n')


def df_colname_munge(df_name):
    """此函数用于将列名进行处理，
    去掉左右多余的字符和空格"""
    pattern = '[(), :;%/.+–-]'  # 定义要去掉的左右字符
    col_list_raw = list(df_name.columns)
    col_list = []
    for col_name in col_list_raw:
        cln_col_name = col_name.strip(pattern)    # 将列名进行处理，去掉左右多余的符号
        col_list.append(cln_col_name)
    return col_list


df.columns = df_colname_munge(df)     # 得到干净的字段名称
print('Clean column names successfully!')


def df_data_munge(df_name):
    """此函数用于将字符类型的字段值进行处理，
        去掉多余的字符和空格"""
    pattern = '[(), :;%/.+–-]'  # 定义要去掉的左右字符
    col_list = list(df_name.columns)  # 获取完整列名
    # noinspection PyBroadException
    try:
        col_list.remove('客房总数')       # 只处理字符类型的列，"客房总数"的数据类型是int
    except AttributeError:
        print('\nWarning: can not remove column "客房总数". Maybe it does not exist in the input file.\n')
        pass
    count = 0
    for col in col_list:
        for index_i, row_i in df_name.iterrows():
            df_name.loc[index_i, col] = row_i[col].strip(pattern)   # 去掉左右多余字符包括空格
        print('Data munge for column %s is finished!\n' % col)
        count = count + 1
    print('Done! Total %s columns of text type are processed. ' % count)
    return df_name


df_data_munge(df)    # 字符类型的字段值处理完毕
print('Clean column values of text type successfully!')


def key_word_check(df_name, sql_engine, target_table, buff_table):
    """
    使用hotel_id, hotel_name, 酒店名称这几个字段将待写入的数据与数据库原有值进行比较，
    正确就进行备份，否则退出并给出错误提示
    df_name: 输入数据的DataFrame
    sql_engine: sqlalchemy创建的数据库引擎
    target_table: 需要更新的数据库，需要指定schema
    buff_table: 用于备份的表格名称

    """
    for index_j, row_j in df_name.iterrows():
        read_sql_com = '''SELECT * FROM %s WHERE hotel_id = '%s';''' % (target_table, row_j['hotel_id'])
        df_ref = pd.read_sql(read_sql_com, con=sql_engine)  # 将数据库里原来的数据取出
        print('\nRow %s\n' % index_j)
        cond1 = (row_j['hotel_id'] == df_ref.loc[0, 'hotel_id'])
        cond2 = (row_j['hotel_name'] == df_ref.loc[0, 'hotel_name'])
        cond3 = (row_j['酒店名称'] == df_ref.loc[0, '酒店名称'])
        if cond1 and cond2 and cond3:
            df_ref.to_sql(buff_table, con=sql_engine, schema='buffer', index=False, if_exists='append')
            print('Old data copied to buffer!\n')
            print(df_ref)
        else:
            print('Warning: data mismatch found！ Pls check input row for hotel_id = \'%s\'\n' % row_j['hotel_id'])
            print(row_j.to_frame().T)


key_word_check(df, engine, 'public.hotel_test', 'hotel_test')
# TO DO: 检查失败的行从DataFrame中删除
print('Key word check finished successfully!')


def update_database(df_name, sql_conn, sql_cursor, updated_table, primary_key, keyword_1, keyword_2):
    """使用输入数据覆盖数据库原来的记录，并加入时间戳和操作员，参数说明如下：
    df_name: 由输入数据转换过来的DataFrame
    sql_conn, sql_cursor: psycopg2_cur() 函数生成的库连接和游标对象
    updated_table: 目标表格
    primary_key, keyword_1, keyword_2: 不能覆盖的数据，只用于数据检查
    """
    df_col_list = list(df_name.columns)
    df_col_list.remove(primary_key)
    df_col_list.remove(keyword_1)
    df_col_list.remove(keyword_2)
    print('\nStart to update designated columns: \n %s' % df_col_list)
    count = 0
    for updated_col in df_col_list:
        for index_k, row_k in df_name.iterrows():
            sql_com_1 = '''UPDATE {} SET "{}" = %s WHERE hotel_id = %s; '''.format(updated_table, updated_col)
            sql_cursor.execute(sql_com_1, (row_k[updated_col], row_k[primary_key]))
            count += 1
    sql_conn.commit()
    for index_n, row_n in df_name.iterrows():            # 加入时间戳和修改用户
        sql_com_2 = '''UPDATE {} SET "生效时间" = current_timestamp , "修改用户" = current_user
                    WHERE {} = '{}'; '''.format(updated_table, primary_key, row_n[primary_key])
        sql_cursor.execute(sql_com_2)
    sql_conn.commit()
    print('\n\nDone! Total updated values: %s\n\n' % count)


update_database(df, con, cur, 'public.hotel_test', 'hotel_id', 'hotel_name', '酒店名称')
print('Database updated successfully!')

# TO DO: 将buffer schema里table里的数据复制到history schema里的对应table，并加入时间戳和操作员

con.close()



