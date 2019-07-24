"""
版本v1说明：
此版本对数据库进行数据更新，
同时调用postgresql的trigger功能对被改动的数据进行备份
"""

import os
import time
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

print("Step 1: import necessary modules --- done!\n\n")


def psycopg2_cur():  # 创建psycopg2游标对象
    conn = psycopg2.connect(database="hami",
                            user="davis",
                            password="zhangyi1212",
                            host="127.0.0.1",
                            port="5432")

    return conn.cursor(), conn


def sqlalchemy_engine():   # 创建sqlalchemy引擎
    return create_engine('postgresql://davis:zhangyi1212@localhost:5432/hami')


print('Step 2: connect to database...\n')
cur, con = psycopg2_cur()                  # for executing sql command
engine = sqlalchemy_engine()          # for DataFrame
print("Step 2 is done!\n\n")


def full_input_path(file_name):
    """
    This  func. is called by get_input_df()
    :param file_name:
    :return: file name with abs. path
    """
    print('Pls specify the path of input file ...')
    print('Example: D:\\file\\python\\AutomatePDF\n')
    path = input()  # call for input
    source_folder = os.path.abspath(path)  # input path must be abs. path
    abs_path = os.path.join(source_folder, file_name)
    return abs_path


def get_input_df(src_file):   # transform input excel file into DataFrame
    """
    :param src_file:  source file
    :return: DataFrame
    """
    input_file = full_input_path(src_file)      # get full abs. path
    obj = pd.ExcelFile(input_file)  # create excel obj.
    df_local = pd.read_excel(obj, 'Sheet1')    # read excel data and return DataFrame
    return df_local


print('Step 3: transform input data to DataFrame...\n')
sourceFile = 'update_hotel.xlsx'  # input file name
df = get_input_df(sourceFile)   # get DataFrame
print("Step 3 is done!\n\n")


def df_colname_munge(df_name):
    """
    此函数用于将列名进行处理，
    去掉左右多余的字符和空格
    :param df_name: 要处理的DataFrame
    :return: column list
    """
    pattern = '[(), :;%/.+–-]'  # 定义要去掉的左右字符
    col_list_raw = list(df_name.columns)  # 将DataFrame的列名转为list
    col_list = []
    for col_name in col_list_raw:
        cln_col_name = col_name.strip(pattern)    # 将列名进行处理，去掉左右多余的符号
        col_list.append(cln_col_name)
    return col_list


print('Step 4: clean column names...\n')
df.columns = df_colname_munge(df)     # 得到干净的字段名称
print('Step 4 is done!\n\n')


def df_data_munge(
                                    df_name,
                                    non_char_col=('客房总数',)
                                    ):
    """
    此函数用于将字符类型的值进行处理，
    去掉多余的字符和空格
    :param df_name: 待处理的DataFrame
    :param non_char_col: list type，非字符型字段列表
    :return:
    """

    pattern = '[(), :;%/.+–-]'  # 定义要去掉的左右字符
    col_list = list(df_name.columns)  # 获取完整列名
    cnt_i = 0
    for item in non_char_col:
        try:
            col_list.remove(item)       # 删除非字符类型的列
            cnt_i += 1
        except AttributeError:
            print("""
                    Warning: can not remove column "{}"!
                    Maybe it does not exist in the input file.\n""".format(item))
            continue
    print('Remove total %s non-character columns...' % cnt_i)

    cnt_j = 0
    for col in col_list:
        for index_i, row_i in df_name.iterrows():
            df_name.loc[index_i, col] = row_i[col].strip(pattern)   # 去掉左右多余字符包括空格
        print('Data munge for column %s is finished!\n' % col)
        cnt_j += 1

    print('Done! Total %s columns of text type are processed. ' % cnt_j)
    return df_name


print('Step 5: clean data value of character type...\n')
df_data_munge(
                            df,
                            non_char_col=('客房总数',)
                            )

print('Step 5 is done!\n\n')


def key_word_check(
                    df_name,
                    table,
                    schema='public',
                    sql_engine=engine,
                    pm_key='hotel_id',
                    comp_wd=('hotel_id', 'hotel_name', '酒店名称')
                   ):
    """
    使用hotel_id, hotel_name, 酒店名称这几个字段将待写入的数据与数据库原有值进行比较
    :param df_name: 待处理的DataFrame
    :param table: 用于备份的表格名称
    :param schema: 需要更新的数据库，指定schema
    :param sql_engine: sqlalchemy创建的数据库引擎
    :param pm_key: 数据库表的primary key
    :type comp_wd: 用于比较的关键词
    :return:
    """
    ful_tab_nam = schema + '.' + table
    for index_j, row_j in df_name.iterrows():
        read_sql_com = '''SELECT * FROM %s WHERE %s = '%s'; 
                        ''' % (ful_tab_nam,
                               pm_key,
                               row_j[pm_key])

        df_ref = pd.read_sql(read_sql_com, con=sql_engine)  # get one row from sql
        print('\nRow %s:  \n' % index_j)
        comp_result = True
        pbm_clmn = 'No warning!'    # 先初始化这个值，因为for loop里的变量会可能出现未赋值情况
        for kw in comp_wd:    # check key words
            comp_result = comp_result and (row_j[kw] == df_ref.loc[0, kw])
            if not comp_result:
                pbm_clmn = 'Warning: %s does match between input and target table' % kw
                print(pbm_clmn)
                break

        if comp_result:        # 正确的数据写入buffer
            print('Check result: correct!\n')

        else:
            print('''Warning: data mismatch found！ Pls check input row for %s = '%s'
                    ''' % (pm_key,
                           row_j[pm_key]))
            problem_row = row_j.to_frame().T
            print(problem_row)
            with open(r'C:\sql\sql_update_log.txt', 'w') as f:    # 有问题的数据写入log文件
                time_stamp: str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                f.write(time_stamp + '\n')
                print(str(problem_row), file=f)  # problem_row is DataFrame
                f.write('\n' + pbm_clmn + '\n')

            df_name.drop([index_j], inplace=True)


print('Step 6: check key words...\n')
key_word_check(df, table='hotel_test')
print('Step 6 is done!\n\n')


def update_database(df_name,
                    target_table,
                    schema,
                    sql_conn=con,
                    sql_cursor=cur,
                    primary_key='hotel_id',
                    keywords=('hotel_name', '酒店名称'),
                    ):
    """
    使用输入数据覆盖数据库原来的记录，并修改时间戳和操作员
    :param target_table: 要更新的表格名称
    :param schema:  表格所在的schema
    :param keywords:
    :param df_name: 输入数据的DataFrame
    :param sql_conn: sql数据库连接，psycopg2
    :param sql_cursor: 连接数据库的游标，用于执行sql语句，psycopg2
    :param primary_key: 表格的primary key
    :param keywords: 不能覆盖的数据，仅用于检查
    :return:
    """
    tm = [""" "生效时间" = current_timestamp """]
    usr = [""" "修改用户" = current_user """]

    updated_table = schema + '.' + target_table
    df_col_list = list(df_name.columns)
    rm_clmn = [primary_key] + list(keywords)
    for item in rm_clmn:
        try:
            df_col_list.remove(item)
        except AttributeError:
            print('Note: keyword %s does not exist!\n' % item)

    print('\nStart to update designated columns: \n %s' % df_col_list)

    count = 0    # 初始化计数器，用于统计更新数据

    for index_k, row_k in df_name.iterrows():
        col_coms = []
        for updated_col  in  df_col_list:
            col_coms_i = """      "{0}" = {1}   """.format(
                                                                                        updated_col,
                                                                                        row_k[updated_col])
            col_coms += [col_coms_i]

        col_coms += tm
        col_coms += usr

        sql_comm_cols = ','.join(col_coms)

        sql_com_fn = """ 
                                UPDATE {0} 
                                SET  {1}
                                WHERE "{2}" = '{3}'; 
                                """.format(
                                                updated_table,
                                                sql_comm_cols,
                                                primary_key,
                                                row_k[primary_key]
                                                )

        sql_cursor.execute(sql_com_fn)
        sql_conn.commit()

    print('\n\nDone! Total updated values: %s\n\n' % count)


print('Step 7: updating database...\n')
update_database(
                df,
                target_table='hotel_test',
                schema='public')

print('Step 7 is done!\n\n')


con.close()
print("""
        ------- Data update is finished successfully!
        ------- Script reaches the end!
        """)
