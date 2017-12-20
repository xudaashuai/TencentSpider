# coding:utf-8
import base64
import hashlib
from io import BytesIO

from scrapy import *
from scrapy.exceptions import CloseSpider
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
import json, psycopg2, re
# 15120954
import sys

conn = psycopg2.connect("host=localhost port=5439 user=postgres dbname=postgres")
cur=conn.cursor()
key = ['AFLBZ-2LJRU-AFKVM-4FOWG-SPY5F-EIBAK',
       'ZHXBZ-IGXWW-QCXRZ-RHKET-AKJL5-IXF3P',
       'GHQBZ-2TTW6-H3BSF-MKEDU-ISHBJ-LRB2J',
       'VC4BZ-3FO3I-IPVGB-5UXHC-IYJSE-T4B3L',
       'KUIBZ-YU66I-KHAGH-5TF77-LOQSO-NHFXN',
       'XATBZ-VQ5CO-HKPWI-SMCF5-JOPGE-JLFOS',
       '6O4BZ-H5R3G-362QG-IKR4C-AZDDH-4GFRJ',
       'DUWBZ-LDYCW-ETLRF-OP7ZE-KHJQS-ILBN5',
       '7ZKBZ-77NK6-I3HSB-M7RRD-T4JBF-CWFI4',
       'RZWBZ-KN5KU-4UAVM-2W2MY-7ECN2-K2FMZ',
       'PMCBZ-HTJKP-KWIDV-LXKHE-Y2LOS-A6FAE',
       'AFLBZ-2LJRU-AFKVM-4FOWG-SPY5F-EIBAK',
       'NQLBZ-Q2FKS-LBUOX-6H3VZ-3FI35-KQFRC',
       'RLDBZ-6CP6P-WSJDZ-LUQNM-LMDR3-SEBMY',
       'KT3BZ-PKWCP-YTTDB-VFDGS-R5EF5-ASB67',
       'DNPBZ-3WXC4-YIGUO-DAZGD-VGBTJ-SZBA6',
       '7EGBZ-G3LKP-3ZQD5-VSRDW-7WTFO-JLBXV',
       'DY7BZ-JRX6P-IXCDP-VXYMG-ODIKV-NYFAJ',
       'KFYBZ-3H4CU-BLCVO-4FYVI-4LZ5H-Y5FDL',
       '6XDBZ-6ANC3-F5Z3Z-3CVPN-5MSEZ-7RB3S',
       'NQLBZ-Q2FKS-LBUOX-6H3VZ-3FI35-KQFRC']

api_url=u'http://apis.map.qq.com/ws/streetview/v1/image?' \
        u'size=960x640&' \
        u'pano={0}&' \
        u'pitch=0&' \
        u'heading=0&' \
        u'key={1}'
class PictureSpider(Spider):
    name = 'Pic'
    index = 0
    ind=0
    def start_requests(self):
        cur.execute("select panoid,lat,lng,id from poi.bj_tencentpois where panoid  not like '' and pic != 5")
        panoids = cur.fetchall()
        for i in range(len(panoids)):
            panoid=panoids[i][0]
            if panoid.__len__()<=1:
                continue
            if i % 100 == 0:
                print(i / 100)
            yield Request(api_url.format(panoid, key[self.index]),
                          self.parse, meta={'id':panoids[i][3],'panoid': panoid,'lat':panoids[i][1],'lng':panoids[i][2], 'index': self.index})
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
            f2=open('I:/北京腾讯街景图片/'+'{0}_{1}_{2}'.format(meta['id'],meta['lat'],meta['lng'])+'.jpg','wb+')
            f2.write(response.body)

            f2.flush()
            f2.close()
            self.ind+=1
            if self.ind % 100 == 0:
                print(self.ind)
            cur.execute("update poi.bj_tencentpois set pic=2 where panoid = %s",(meta['panoid'],))
            conn.commit()
def save(pic_bin):

    # 读取exif 数据
    pic_file = BytesIO(pic_bin)  # 将二进制数据转化成文件对象便于读取exif数据信息和生成MD5

    # 生成MD5
    MD5 = hashlib.md5(pic_file.read()).hexdigest()
    # 首先把二进制图片用base64 转成字符串之后再存
    # 存图片
    cursor.execute(
        INSERT_PICS.format(city_pics=self.city_pics,
                           pic_url=pic_url['thumbnail_pic'].replace('thumbnail',
                                                                    'large'),
                           pic_bin=str(base64.b64encode(pic_bin))[2:-1], md5=MD5,
                           exif=tags))

    conn.commit()