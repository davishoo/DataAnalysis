def cln_col_nam(df_name):
    """
    此函数用于将DataFrame的列名进行处理，
    去掉左右多余的字符和空格
    :param df_name: DataFrame
    :return: None
    """
    pattern = '[( )（）,:;%/.+–-]'  # 定义要去掉的左右字符
    col_list_old = list(df_name.columns)  # 将DataFrame的列名转为list
    col_list_new = []
    for col_name in col_list_old:
        cln_nam = col_name.strip(pattern)    # 将列名进行处理，去掉左右多余的符号
        col_list_new.append(cln_nam)

    df_name.columns = col_list_new
    return None


def cln_str_val(df_name, non_str_col):
    """
    此函数用于将字符类型的值进行处理，
    去掉多余的字符和空格
    :param df_name: 待处理的DataFrame
    :param non_str_col: tuple
    :return: None
    """
    pattern = '[( )（）,:;%/.+–-]'  # 定义要去掉的左右字符
    str_columns = list(df_name.columns)  # 获取完整列名
    cnt = 0
    for item in non_str_col:
        try:
            str_columns.remove(item)       # 删除非字符类型的列
            cnt += 1
        except AttributeError:
            print(
                    f"""
                    Warning: can not remove non-str column "{item}"!
                    Maybe it does not exist in the input file.\n"""
                    )
            continue

    print(f'Remove total {cnt} non-character columns...')

    cnt = 0
    for i in str_columns:
        df_name[i] = df_name[i].str.strip(pattern)
        cnt += 1

    print(f'Done! Total {cnt} columns of str type are stripped.\n\n ')
    return None


def rem_duplicates(df_name, keys):
    """
    remove duplicated rows specified by hotel_name, 酒店名称, 详细地址
    :param df_name:  DataFrame
    :param keys:  tuple, e.g ('hotel_name', '酒店名称')
    :return: None
    """
    for i in keys:
        df_name.drop_duplicates([i], keep='first', inplace=True)

    return None
