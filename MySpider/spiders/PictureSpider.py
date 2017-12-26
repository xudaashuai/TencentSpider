# coding:utf-8
import base64
import hashlib
from io import BytesIO

from scrapy import *
from scrapy.exceptions import CloseSpider
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
import json, psycopg2, re,pymysql
conn = pymysql.connect(host='localhost',port=3306,user='root',passwd='admin',db='tencent',charset='utf8')
cur = conn.cursor()
# 15120954
import sys
INSERT_PICS = 'insert into tencent.pano VALUES ("{id}",{lat},{lng},"{desc}",{heading},{pitch},{zoom},{pov_exp},"{pic_bin}")'
key = ['AFLBZ-2LJRU-AFKVM-4FOWG-SPY5F-EIBAK']

pano_url='http://apis.map.qq.com/ws/streetview/v1/getpano?id={0}&key={1}'
api_url='http://apis.map.qq.com/ws/streetview/v1/image?' \
        'size=960x640&' \
        'pano={0}&' \
        'pitch=0&' \
        'heading=0&' \
        'key={1}'
class PictureSpider(Spider):
    name = 'Pic'
    index = 0
    ind=0
    def start_requests(self):
        cur.execute("select panoid,lat,lng,id from poi.bj_tencentpois where panoid  not like '' and pic != 2")
        panoids = cur.fetchall()
        for i in range(len(panoids)):
            panoid=panoids[i][0]
            if panoid.__len__()<=1:
                continue
            if i % 100 == 0:
                print(i / 100)
            yield Request(pano_url.format(panoid, key[self.index]),
                          self.parse_pano, meta={'id':panoids[i][3],'panoid': panoid,'lat':panoids[i][1],'lng':panoids[i][2], 'index': self.index})
    def parse_pano(self,response):

        j = json.loads(response.body.decode('utf-8'))
        if j['status']==0:
            yield Request(api_url.format(response.meta['panoid'], key[self.index]),self.parse, meta=response.meta)
            t = 'insert poi.pano (id,lat,lng,`desc`) values("{0}",{1},{2},"{3}")'.format(response.meta['panoid'],j['detail']['location']['lat'],j['detail']['location']['lng'],j['detail']['description'])
            #print(t)
            cur.execute(t)
            conn.commit()
    def parse(self, response):
        meta=response.meta
        if response.body.__len__()<2000:
            j=json.loads(response.body.decode('utf-8'))
            print(j)
            if j['status'] == 121:
                meta['index'] += 1
                self.index=meta['index']
                print(self.index ,'key 结束啦')
                if meta['index'] >= key.__len__():
                    raise CloseSpider('key 用完了耶')
            elif j['status'] == 113:
                print(self.index ,'key 有问题')
                meta['index'] += 1
                self.index=meta['index']
            elif j['status'] == 383:
                print(self.index, 'pano 有问题')
                cur.execute("update poi.bj_tencentpois set pic=-1 where panoid = %s",(meta['panoid'],))
                conn.commit()
        else:
            self.ind+=1
            if self.ind % 100 == 0:
                print(self.ind)
            cur.execute("update poi.bj_tencentpois set pic=2 where panoid = %s",(meta['panoid'],))
            save(response.body,meta['panoid'])
            conn.commit()
def save(pic_bin,panoid):

    # 读取exif 数据
    pic_file = BytesIO(pic_bin)  # 将二进制数据转化成文件对象便于读取exif数据信息和生成MD5

    # 生成MD5
    MD5 = hashlib.md5(pic_file.read()).hexdigest()
    # 首先把二进制图片用base64 转成字符串之后再存
    # 存图片
    #cur.execute(
     #   INSERT_PICS.format(city_pics=self.city_pics,
      #                                                       'large'),
       #                    pic_bin=str(base64.b64encode(pic_bin))[2:-1], md5=MD5,
        #                   exif=tags))
    cur.execute("update poi.pano set pic_bin = %s where id = %s", (str(base64.b64encode(pic_bin))[2:-1],panoid,))
    conn.commit()