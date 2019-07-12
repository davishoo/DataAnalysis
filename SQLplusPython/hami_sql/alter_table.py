import psycopg2

con = psycopg2.connect(database="hami", user="davis", password="zhangyi1212", host="127.0.0.1", port="5432")
print("Database opened successfully!\n\n")
cur = con.cursor()

sql = '''ALTER TABLE history.hotel_test1
        ADD COLUMN "effective_time" timestamp with time zone NOT NULL DEFAULT current_timestamp;

        ALTER TABLE history.hotel_test1
        ADD COLUMN "effective_by"  text NOT NULL DEFAULT current_user;'''

cur.execute(sql)

con.commit()
con.close()


print('\n\nDone! \n\n')

# above code works




