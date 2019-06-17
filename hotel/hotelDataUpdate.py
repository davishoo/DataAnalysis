import pandas as pd
hotel = pd.ExcelFile('hotelData.xlsx')
hotelDF = pd.read_excel(hotel, 'Sheet1')

level_1 = (('基本信息',) * 5) + (('酒店销售',) * 6)
level_2 = ('酒店名称', '酒店位置', '详细地址', '前台电话', '客房总数', '姓名', 
                    '手机', '座机', '微信','QQ','Email')

hotelDF.columns = [list(level_1), list(level_2)]

writer = pd.ExcelWriter('酒店完整信息表.xlsx')
hotelDF.to_excel(writer, 'Sheet1')
writer.save()

writer = pd.ExcelWriter('酒店基本信息表.xlsx')
hotelDF.基本信息.to_excel(writer, 'Sheet1', index = False)
writer.save()

print('done')
