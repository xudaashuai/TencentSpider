from scrapy import *
from scrapy.exceptions import CloseSpider
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
import requests
import json, psycopg2, re,datetime
# 15120954
import sys
ind = 0
conn = psycopg2.connect("host=localhost port=5439 user=postgres dbname=postgres")
cur=conn.cursor()
key = ['AFLBZ-2LJRU-AFKVM-4FOWG-SPY5F-EIBAK']

def parse(response, ind,meta):
    if response.content.__len__() < 2000:
        j = json.loads(response.body.decode('utf-8'))
        print(j)
        if j['status'] == 121:
            print('key 结束啦')
            exit()
        elif j['status'] == 383:
            print( 'pano 有问题')
            cur.execute("update poi.bj_tencentpois set pic=-1 where panoid = %s", (meta[0],))
            conn.commit()
        else:
            print(j)
    else:
        f2 = open('G:/t/' + '{0}_{1}_{2}'.format(meta[0], meta[1], meta[2]) + '.jpg', 'wb+')
        f2.write(response.content)
        f2.flush()
        f2.close()
        print(datetime.datetime.now(),ind)
        cur.execute("update poi.bj_tencentpois set pic=2 where panoid = %s", (meta[0],))
        conn.commit()
api_url=u'http://apis.map.qq.com/ws/streetview/v1/image?' \
        u'size=960x640&' \
        u'pano={0}&' \
        u'pitch=0&' \
        u'heading=0&' \
        u'key={1}'
cur.execute("select panoid,lat,lng,id from poi.bj_tencentpois where panoid not like '' and pic = 0")
panoids = cur.fetchall()
for i in range(len(panoids)):
    panoid = panoids[i][0]
    if panoid.__len__() <= 1:
        continue
    if i % 100 == 0:
        print(i / 100)
    res = requests.get(api_url.format(panoid, key[0]))
    parse(res,ind,panoids[i])

