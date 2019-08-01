
def src_path(file):
    """
    :param file:
    :return: file name with abs. file  path
    """
    import os
    print('Pls specify the path of input file ...')
    print('Example: D:\\file\\python\\AutomatePDF\n')
    path = input()  # call for input
    src_dir = os.path.abspath(path)  # make sure the path is absolute one
    file_path = os.path.join(src_dir, file)
    return file_path


def get_df(src_pth):   # transform input excel file into DataFrame
    """
    :param src_pth:  source file with abs. path
    :return: DataFrame
    """
    import pandas as pd
    obj = pd.ExcelFile(src_pth)  # create excel obj.
    df_local = pd.read_excel(obj, 'Sheet1')    # read excel data and return DataFrame
    return df_local
