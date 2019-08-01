import pandas as pd
from sqlalchemy import create_engine


def rem_dup(df, tab, schema, keys):
    """
    compare input df with sql db;
    remove input row if it already exists in db
    :param df:   DataFrame  to be processed
    :param tab:  str, table name
    :param schema: str
    :param keys: tuple, e.g.  keys=('hotel_name', '酒店名称')
    :return:
    """
    pw = input('Pls input password for accout "davis": ')
    sql_engine = create_engine(f'postgresql://davis:{pw}@localhost:5432/hami')
    qual_tab = schema + '.' + tab

    for key in keys:
        cmd = f" SELECT {key} FROM {qual_tab} ; "
        sql_df = pd.read_sql(sql=cmd, con=sql_engine)

        for i, row in df.iterrows():
            if row[key] in sql_df[key].tolist():
                df.drop([i], inplace=True)
            else:
                pass


def add_pkey(df, tab, schema, pkey, prefix):
    """

    :param df:
    :param tab:
    :param schema:
    :param pkey:  primary key
    :param prefix:  prefix string, e.g. 'HK-'
    :return:
    """
    pw = input('Pls input password for accout "davis": ')
    sql_engine = create_engine(f'postgresql://davis:{pw}@localhost:5432/hami')

    qual_tab = schema + '.' + tab
    cmd = f" SELECT {pkey} FROM {qual_tab} ; "
    sql_df = pd.read_sql(sql=cmd, con=sql_engine)

    if len(sql_df[pkey]) == 0:
        id_max = 0
    else:
        id_str = sql_df[pkey].str.lstrip(prefix)
        id_int = id_str.apply(int)
        id_max = id_int.max()

    print(f'Current max.id in sql DB is: {id_max}')

    pk = []
    for i in range(len(list(df.index))):
        pk.append('HK-' + str(i + 1 + id_max).zfill(4))

    df[pkey] = pk


def check_kwd(df_name, table, schema, pkey, kwd):
    """
    使用kwd这几个字段将待写入的数据与数据库原有值进行比较
    :param df_name: 待处理的DataFrame
    :param table: 目标表格名称
    :param schema: 需要更新的数据库，指定schema
    :param pkey: 数据库表的primary key
    :param kwd: tuple, 用于比较的关键词
    :return:
    """
    import time
    pw = input('Pls input password for account "davis": ')
    sql_engine = create_engine(f'postgresql://davis:{pw}@localhost:5432/hami')

    qual_tab = schema + '.' + table
    for idx, row in df_name.iterrows():
        comm = f''' SELECT * FROM {qual_tab} WHERE {pkey} = '{row[pkey]}'; '''
        reference = pd.read_sql(comm, con=sql_engine)  # get one row from sql
        print(f'\nCheck row {idx}...  \n')
        comp_result = True
        pbm_clmn = 'No error found!'    # 先初始化这个值，因为for loop里的变量会可能出现未赋值情况
        for k in kwd:    # check key words
            comp_result = (comp_result and (row[k] == reference.loc[0, k]))
            if not comp_result:
                pbm_clmn = f'Warning: found mismatch of "{k}"  between input and target table'
                print(pbm_clmn)
                break

        if comp_result:        # 正确的数据写入buffer
            print(f' Congrats! Row {idx} passes key-word check!\n')

        else:
            print(
                f'''Warning: data mismatch found！Pls check input row for {pkey} = '{row[pkey]}' '''
                )
            pbm_row = row.to_frame().T
            print(pbm_row)
            with open(r'C:\sql\update_hotels_log.txt', 'a') as log_f:    # 有问题的数据写入log文件
                print('=' * 50, file=log_f)
                log_f.write(pbm_clmn + '\n')
                ts: str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                log_f.write(ts + '\n')
                print(str(pbm_row), file=log_f)  # problem_row is DataFrame
                print('=' * 50, file=log_f)
                log_f.flush()

            df_name.drop([idx], inplace=True)


def compare_val(df_name, table, schema, pkey, kwd):
    """

    :param df_name: 待处理的DataFrame
    :param table: 目标表格名称
    :param schema: 需要更新的数据库，指定schema
    :param pkey: 数据库表的primary key
    :param kwd: tuple, 用于比较的关键词
    :return:
    """
    import time
    pw = input('Pls input password for account "davis": ')
    sql_engine = create_engine(f'postgresql://davis:{pw}@localhost:5432/hami')

    update_col = list(df_name.columns)
    cnt = 0
    for j in kwd:
        try:
            update_col.remove(j)
            cnt += 1
        except AttributeError:
            print(
                    f"""
                    Warning: can not find key column "{j}"!
                    Maybe it does not exist in the input file.\n"""
                    )
            continue

    print(f'Remove total {cnt} key columns...')

    qual_tab = schema + '.' + table
    for idx, row in df_name.iterrows():
        comm = f''' SELECT * FROM {qual_tab} WHERE {pkey} = '{row[pkey]}'; '''
        reference = pd.read_sql(comm, con=sql_engine)  # get one row from sql
        print(f'\nCheck row {idx}...  \n')
        pbm_clmn = 'No error found!'    # 先初始化这个值，因为for loop里的变量会可能出现未赋值情况
        keep_row = False

        for col_i in update_col:    # check key words
            is_diff = not (row[col_i] == reference.loc[0, col_i])
            keep_row = (keep_row or is_diff)
            if is_diff:
                pass
            else:
                pbm_clmn = f'Warning: the new value of "{col_i}"  is identical to the old one in DB'
                print(pbm_clmn)

        if keep_row:
            print(f' Congrats! Row {idx} passes new  value check!\n')

        else:
            df_name.drop([idx], inplace=True)
            print(
                f'''Warning: input data is the same as DB！Pls check input row for {pkey} = '{row[pkey]}' '''
                )
            pbm_row = row.to_frame().T
            print(pbm_row)
            with open(r'C:\sql\update_hotels_log.txt', 'a') as log_f:    # 有问题的数据写入log文件
                print('*' * 80, file=log_f)
                log_f.write(pbm_clmn + '\n')
                ts: str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                log_f.write(ts + '\n')
                print(str(pbm_row), file=log_f)  # problem_row is DataFrame
                print('*' * 80, file=log_f)
                log_f.flush()
