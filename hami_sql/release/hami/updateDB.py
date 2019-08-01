def update_db(df_name, target_tab, schema, pkey, kwd):
    """
    使用输入数据覆盖数据库原来的记录，并修改时间戳和操作员
    :param df_name: 输入数据的DataFrame
    :param target_tab: 要更新的表格名称
    :param schema:  表格所在的schema
    :param pkey: primary key
    :param kwd: 不能覆盖的数据，仅用于检查
    :return:
    """
    import psycopg2
    pw = input('Pls input password for account "davis": ')
    conn = psycopg2.connect(
                                                    database="hami",
                                                    user="davis",
                                                    password=pw,
                                                    host="127.0.0.1",
                                                    port="5432")
    cur = conn.cursor()

    ts = """ "生效时间" = current_timestamp """
    usr = """ "修改用户" = current_user """

    qual_tab = schema + '.' + target_tab
    clmns = list(df_name.columns)         # clmns stands for columns to be updated
    rem = [pkey] + list(kwd)
    for i in rem:
        try:
            clmns.remove(i)
        except AttributeError:
            print(f'Note: column {i}%s does not exist!\n')
            continue

    print(f'\nStart to update designated columns: \n {clmns}')

    count = 0    # 初始化计数器，用于统计更新数据
    for idx, row in df_name.iterrows():
        col_cmd = []                 # cms stands for commands
        for col in clmns:
            col_cmd_i = f""" "{col}" = '{row[col]}'  """
            col_cmd.append(col_cmd_i)

        col_cmd.append(ts)
        col_cmd.append(usr)

        print(f'col_cmd = : \n{col_cmd}')  # check point 1

        col_cmd_str = ','.join(col_cmd)       # merge all elements into a string

        print(f'col_cmd_str = : \n{col_cmd_str}')  # check point 2

        whole_cmd = f""" UPDATE {qual_tab}    SET  {col_cmd_str}
                                         WHERE "{pkey}" = '{row[pkey]}';   """

        print(f'whole_cmd = : \n{whole_cmd}')  # check point 3

        cur.execute(whole_cmd)
        conn.commit()
        count += 1

    conn.close()

    print(f'\nDone! Total updated rows: {count}\n\n')
