import sys
# import my  own modules
sys.path.append(r"C:\Users\deand\PycharmProjects\hami_sql\release")
from hami import sourcing, dataClean, compToDB, updateDB

b_line = "*"*60

print("==> Step 1: import modules --- done!\n")

print(b_line)
# =========================================================
print('\n==> Step 2: transform input data to DataFrame...\n')

in_file = 'update_hotel.xlsx'
src = sourcing.src_path(in_file)
df_in = sourcing.get_df(src)

print("==> Step 2 is done!\n")

print(b_line)
# ==========================================================
print('\n==> Step 3: data cleaning...\n')

dataClean.cln_col_nam(
                                                df_name=df_in
                                            )

dataClean.cln_str_val(
                                                df_name=df_in,
                                                non_str_col=('客房总数',)
                                        )

dataClean.rem_duplicates(
                                                df_name=df_in,
                                                keys=('hotel_name', '酒店名称')
                                                )

print('==> Step 3 is done! ******\n')

print(b_line)
# ===========================================================
print('\n==> Step 4: check key words...\n')

compToDB.check_kwd(
                                            df_name=df_in,
                                            table='hotels',
                                            schema='public',
                                            pkey='hotel_id',
                                            kwd=('hotel_id', 'hotel_name', '酒店名称')
                                            )


print('==> Step 4 is done!\n')

print(b_line)
# ===========================================================
print('\n==> Step 5: check new value...\n')

compToDB.compare_val(
                                            df_name=df_in,
                                            table='hotels',
                                            schema='public',
                                            pkey='hotel_id',
                                            kwd=('hotel_id', 'hotel_name', '酒店名称')
                                            )


print('==> Step 5 is done!\n')

print(b_line)
# ==========================================================
print('\n==> Step 6: updating database...\n')

updateDB.update_db(
                                        df_name=df_in,
                                        target_tab='hotels',
                                        schema='public',
                                        pkey='hotel_id',
                                        kwd=('hotel_name', '酒店名称')
                                        )

print('==> Step 6 is done!\n')

print(b_line)
# ==========================================================
print(r"""

        ------- Data update is finished successfully!                     
        ------- Script reaches the end!                                              
        ------- Error log is saved at C:\sql\update_hotels_log.txt    
        """)
