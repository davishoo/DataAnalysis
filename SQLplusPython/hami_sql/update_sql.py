from typing import Any, Union

import pandas as pd
import os
from sqlalchemy import create_engine
import psycopg2
import time
print("Step 1: import necessary modules --- done!")


def psycopg2_cur():  # 创建psycopg2游标对象
    conn = psycopg2.connect(database="hami", user="davis", password="zhangyi1212", host="127.0.0.1", port="5432")
    return conn.cursor(), conn


def sqlalchemy_engine():   # 创建sqlalchemy引擎
    return create_engine('postgresql://davis:zhangyi1212@localhost:5432/hami')


cur, con = psycopg2_cur()
engine = sqlalchemy_engine()
print("Step 2: connect to database --- done!\n\n")


def full_input_path(file_name):
    """
    此函数仅被另外一个函数get_input_df()调用，在主程序里没有被直接调用
    import os
    :param file_name: 文件名
    :return: 将用户输入的路径加上文件名，即文件的绝对路径
    """
    print('Pls specify the path of input file ...')
    print('Example: D:\\file\\python\\AutomatePDF\n')
    path = input()  # call for input
    source_folder = os.path.abspath(path)  # make sure the path is absolute one
    abs_path = os.path.join(source_folder, file_name)
    return abs_path


def get_input_df(src_file):   # 将输入excel数据转换为DataFrame
    """
    import pandas as pd
    :param src_file: 输入数据的文件名
    :return: 返回DataFrame
    """
    input_file = full_input_path(src_file)      # 将输入文件的完整路径赋值给input_file
    obj = pd.ExcelFile(input_file)  # 创建EXCEL目标对象
    df_local = pd.read_excel(obj, 'Sheet1')   # 读取目标对象，返回DataFrame
    return df_local


sourceFile = 'update_hotel.xlsx'  # 定义输入数据的文件名
df = get_input_df(sourceFile)   # 得到DataFrame
print("Step 3: transform input data to DataFrame --- done!\n\n")


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


df.columns = df_colname_munge(df)     # 得到干净的字段名称
print('Step 4: clean column names --- done!')


def df_data_munge(df_name,
                  non_char_col):
    """
    此函数用于将字符类型的值进行处理，
    去掉多余的字符和空格
    :param df_name: 待处理的DataFrame
    :param non_char_col: list type，非字符型字段列表
    :return:
    """

    pattern = '[(), :;%/.+–-]'  # 定义要去掉的左右字符
    col_list = list(df_name.columns)  # 获取完整列名
    # noinspection PyBroadException
    cnt_i = 0
    for item in non_char_col:
        try:
            col_list.remove(item)       # 删除非字符类型的列
        except AttributeError:
            print("""
                    Warning: can not remove column "{}"!
                    Maybe it does not exist in the input file.\n""".format(item))
            continue
        cnt_i += 1
    print('Done! Remove total %s non-character columns' % cnt_i)

    cnt_j = 0
    for col in col_list:
        for index_i, row_i in df_name.iterrows():
            df_name.loc[index_i, col] = row_i[col].strip(pattern)   # 去掉左右多余字符包括空格
        print('Data munge for column %s is finished!\n' % col)
        cnt_j += 1

    print('Done! Total %s columns of text type are processed. ' % cnt_j)
    return df_name


df_data_munge(df,
              non_char_col=['客房总数']
              )

print('Step 5: clean column values of character type --- done!')


def key_word_check(df_name,
                   table,
                   target_schema='public.',
                   sql_engine=engine,
                   pm_key='hotel_id',
                   comp_wd=('hotel_id', 'hotel_name', '酒店名称')
                   ):
    """
    使用hotel_id, hotel_name, 酒店名称这几个字段将待写入的数据与数据库原有值进行比较，
    正确就复制到buffer，否则退出并给出错误提示
    :param df_name: 待处理的DataFrame
    :param table: 用于备份的表格名称
    :param target_schema: 需要更新的数据库，指定schema
    :param sql_engine: sqlalchemy创建的数据库引擎
    :param pm_key: 数据库表的primary key
    :type comp_wd: 用于比较的关键词
    :return:
    """
    ful_tab_nam = target_schema + table
    for index_j, row_j in df_name.iterrows():
        read_sql_com = '''SELECT * FROM %s WHERE %s = '%s'; 
                        ''' % (ful_tab_nam,
                               pm_key,
                               row_j[pm_key])

        df_ref = pd.read_sql(read_sql_com, con=sql_engine)  # 将数据库里原来的数据取出，只有一行
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
            df_ref.to_sql(table,
                          con=sql_engine,
                          schema='buffer',
                          index=False,
                          if_exists='append')
            print('Old data copied to buffer!\n')
            print(df_ref)
        else:
            print('''Warning: data mismatch found！ Pls check input row for %s = '%s'
                    ''' % (pm_key,
                           row_j[pm_key]))
            problem_row = row_j.to_frame().T
            print(problem_row)
            with open(r'C:\sql\sql_update_log.txt', 'w') as f:    # 有问题的数据写入log文件
                time_stamp: str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                f.write(time_stamp + '\n')
                f.write(problem_row + '\n')
                f.write(pbm_clmn + '\n')

            df_name.drop([index_j], inplace=True)


key_word_check(df, table='hotel_test')

print('Step 6: check key word --- done!')


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
    updated_table = schema + '.' + target_table
    df_col_list = list(df_name.columns)
    rm_clmn = [primary_key] + list(keywords)
    for item in rm_clmn:
        df_col_list.remove(item)

    print('\nStart to update designated columns: \n %s' % df_col_list)

    count = 0    # 初始化计数器，用于统计更新数据
    for updated_col in df_col_list:        # 更新数据库记录
        for index_k, row_k in df_name.iterrows():
            sql_com_1 = """ 
                        UPDATE {0} 
                        SET "{1}" = {2} 
                        WHERE "{3}" = '{4}'; 
                        """.format(updated_table,
                                   updated_col,
                                   row_k[updated_col],   # this is value
                                   primary_key,
                                   row_k[primary_key])   # this is value
            sql_cursor.execute(sql_com_1)
            count += 1
            sql_conn.commit()

    for index_n, row_n in df_name.iterrows():            # 修改时间戳和修改用户
        sql_com_2 = """ 
                    UPDATE {0} 
                    SET "生效时间" = current_timestamp , "修改用户" = current_user
                    WHERE "{1}" = '{2}'; 
                    """.format(updated_table,
                               primary_key,
                               row_n[primary_key])
        sql_cursor.execute(sql_com_2)
        sql_conn.commit()

    print('\n\nDone! Total updated values: %s\n\n' % count)


update_database(df, target_table='hotel_test', schema='public')
print('Step 7: update database --- done!')

# 以下代码待确认修改
def backup_old_data(tab,
                    sql_engine=engine,
                    sql_conn=con,
                    sql_cursor=cur,
                    buf_schema='buffer',
                    src_schema='public'):
    """
    将buffer schema里table里的数据复制到
    history schema里的对应table，并加入时间戳和操作员
    :param tab: table name
    :param sql_engine: sql engine, sqlalchemy
    :param sql_conn: sql connection, psycopg2
    :param sql_cursor: sql cursor for 执行sql命令, psycopg2
    :param buf_schema: 存放buffer数据的schema
    :param src_schema: source data schema
    :return:
    """
    qual_buf_tab = buf_schema + '.' + tab
    qual_src_tab = src_schema + '.' + tab
    create_bk_tab = """
                    CREATE TABLE IF NOT EXISTS {0}
                    (
                    LIKE {1}
                    INCLUDING DEFAULTS
                    INCLUDING CONSTRAINTS
                    INCLUDING INDEXES,

                    "disable_time" timestamp with time zone NOT NULL DEFAULT current_timestamp,
                    "disabled_by" text NOT NULL DEFAULT current_user
                    )
                    ;
                    """.format(qualified_tab, source_tab)

    sql_cursor.execute(create_bk_tab)
    sql_conn.commit()

    sql_comm = """SELECT * FROM {}""".format(buf_tab)
    df_buff = pd.read_sql(sql_comm, con=sql_engine)  # 将buff里的数据取出

    # TO TO: 插入2列, "disable_time", "disabled_by"
    df_buff['disable_time'] = pd.NaT
    df_buff['disabled_by'] = None

    # 将DataFrame数据写入备份库
    df_buff.to_sql(buf_tab,
                   con=sql_engine,
                   schema='history',
                   index=False,
                   if_exists='append'
                   )


# TO DO: 清空buffer里的数据
def empty_table(tab_name,
                schema='buffer.',
                sql_conn=con,
                sql_cursor=cur,
                ):
    ful_tab_name = schema + tab_name
    empty_comm = """DELETE FROM {}""".format(ful_tab_name)
    sql_cursor.execute(empty_comm)
    sql_conn.commit()


empty_table('hotel_test')

con.close()
print("""
------- Data update is finished successfully!
------- Script reaches the end!"""
      )


