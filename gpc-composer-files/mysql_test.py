import pandas as pd

print("connecting")


host="35.204.100.0"
database="powerbi_stuff"
user="powerbi"
password="XXXXXXXXXXXXXXXX"



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



d = {}
d["a"] =2
d["b"] =3
d["c"] ="pandas"

df = pd.DataFrame([d,d])
print("create engine")
from sqlalchemy import create_engine
engine=create_engine("mysql://%s:%s@%s/%s" % (user, password, host, database) )
print("write table")
df.to_sql('new_test', engine, if_exists='append')

