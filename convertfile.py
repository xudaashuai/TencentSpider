import psycopg2
import os
conn = psycopg2.connect("host=localhost port=5439 user=postgres dbname=postgres")
cur=conn.cursor()
rootDir='G:/北京腾讯街景图片'

for i,file in enumerate(os.listdir(rootDir)):
    if i%100==0:
        print(i)
    ss=file.split('_')
    ss[-1]=ss[-1].split('.')[0]
    if ss.__len__() ==3:
        cur.execute("update poi.bj_tencentpois set pic = 5 where id = '{0}'".format(ss[0]))
        conn.commit()