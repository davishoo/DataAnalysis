import sys
from sqlalchemy import create_engine

sys.path.append(r"C:\Users\deand\PycharmProjects\hami_sql\release")
from hami import sourcing, dataClean, compToDB

# ===========================================
borderline = '========================================'
print(borderline)
print('****** Step 1: transform input file to DataFrame...\n')

in_file = 'insert_hotelData.xlsx'
src = sourcing.src_path(in_file)
hotelDF = sourcing.get_df(src)

print('****** Step 1 is done! ******\n\n')
print(borderline)
# ===========================================

print('****** Step 2: data cleaning...\n')

dataClean.cln_col_nam(hotelDF)
dataClean.cln_str_val(hotelDF, non_str_col=['客房总数'])

hotelDF.rename(
                                columns={'酒店英文名称': 'hotel_name', '酒店中文名称': '酒店名称'},
                                inplace=True)

dataClean.rem_duplicates(hotelDF, ('hotel_name', '酒店名称'))

print('****** Step 2 is done! ******\n\n')
print(borderline)
# ===========================================

print('****** Step 3: compare to sql database...\n')

compToDB.rem_dup(
                                        df=hotelDF,
                                        tab='hotels',
                                        schema='public',
                                        keys=('hotel_name', '酒店名称'))

print('****** Step 3 is done! ******\n\n')
print(borderline)
# ============================================

print('****** Step 4: add primary key for each row...\n')

compToDB.add_pkey(
                                        df=hotelDF,
                                        tab='hotels',
                                        schema='public',
                                        pkey='hotel_id',
                                        prefix='HK-'
                                        )

print('****** Step 4 is done! ******\n\n')
print(borderline)
# ============================================
print('****** Step 5: inserting data...\n')

hotels = hotelDF[[
                                'hotel_id', 'hotel_name', '酒店名称',
                                '城市', '酒店位置', '详细地址', '客房总数']]


pwd = input('Pls input password for accout "davis": ')
engine = create_engine(f'postgresql://davis:{pwd}@localhost:5432/hami')

hotels.to_sql(
                        'hotels',
                        con=engine,
                        schema='public',
                        index=False,
                        if_exists='append')

print('\n****** Step 5: data insertion finished! ******\n')
print(borderline)
# =============================================

print('++++++ END OF PROGRAM ++++++')
