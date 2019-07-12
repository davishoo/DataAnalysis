import pandas as pd
import os


def source_path():
    # import os
    print('Pls specify the path of input file ...')
    print('Example: D:\\file\\python\\AutomatePDF\n\n')
    path = input()   # call for input
    source_folder = os.path.abspath(path) # make sure the path is absolute one
    return source_folder


sourceFile = 'hotelData.xlsx' 
absPath = os.path.join(source_path(), sourceFile)

    
inputObj = pd.ExcelFile(absPath)    # specify input data source
hotelDF = pd.read_excel(inputObj, 'Sheet1')

level_1 = ('Key',) * 2 + ('基本信息',) * 4 + ('酒店销售',) * 6              # define column name of level_1
level_2 = (
            'HotelName', '酒店名称', '酒店位置', '详细地址', '前台电话', '客房总数', '姓名',
            '手机', '座机', '微信', 'QQ', 'Email')      # define column name of level_2

hotelDF.columns = [list(level_1), list(level_2)]  # restructure column names

outputPath = r'C:\Users\deand\pythonScripts\hotel\data'

os.chdir(outputPath) # designate  output  path

hotelDF.to_csv('hotelDB.csv', encoding='utf_8_sig',index=False)  # save to csv

output = pd.read_csv('hotelDB.csv', header=[0, 1])
writer = pd.ExcelWriter('酒店完整信息表.xlsx')
output.to_excel(writer, 'Sheet1')
writer.save()

writer = pd.ExcelWriter('酒店基本信息表.xlsx')
output[['Key', '基本信息']].to_excel(writer, 'Sheet1', index=True)
writer.save()

print('Done!')
print('Output files are located at %s' % outputPath)

