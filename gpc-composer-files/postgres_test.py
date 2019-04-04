import psycopg2
import pandas as pd

print("connecting")

host='35.204.97.179'
database="powerbi-source"
user="postgres"
password="XXXXXXXXXXXXXXXX"

conn = psycopg2.connect(
    host=host,
    database=database,
    user=user,
    password=password)

cur = conn.cursor()


print("create table")
# cur.execute( """
#         CREATE TABLE test (
#             a int,
#             b int,
#             c VARCHAR(255)
#         )
#         """)

# print("commit")
# conn.commit()



cur.execute("INSERT into test (a,b,c) values (1,2, 'hallo' )")
print("commit")
conn.commit()

d = {}
d["a"] =2
d["b"] =3
d["c"] ="pandas"

df = pd.DataFrame([d,d])
from sqlalchemy import create_engine
engine=create_engine("postgres+psycopg2://%s:%s@%s/%s" % (user, password, host, database) )
df.to_sql('new_test', engine, if_exists='append')

print("select version")
cur.execute("SELECT * from test")
print(cur.fetchone())
print("commit")
conn.commit()

cur.execute("SELECT * from new_test")
res = cur.fetchone()
while res != '' and res != None:
    print(res)
    res = cur.fetchone()
