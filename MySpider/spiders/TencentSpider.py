# coding:utf-8
from scrapy import *
from scrapy.exceptions import CloseSpider
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
import json, psycopg2, re
# 15120954
import sys


key = [
       'NQLBZ-Q2FKS-LBUOX-6H3VZ-3FI35-KQFRC',
       '6XDBZ-6ANC3-F5Z3Z-3CVPN-5MSEZ-7RB3S',
       'KFYBZ-3H4CU-BLCVO-4FYVI-4LZ5H-Y5FDL',
    'AFLBZ-2LJRU-AFKVM-4FOWG-SPY5F-EIBAK',
       'PMCBZ-HTJKP-KWIDV-LXKHE-Y2LOS-A6FAE',
       'DY7BZ-JRX6P-IXCDP-VXYMG-ODIKV-NYFAJ',
       '7EGBZ-G3LKP-3ZQD5-VSRDW-7WTFO-JLBXV',
       'DNPBZ-3WXC4-YIGUO-DAZGD-VGBTJ-SZBA6',
       'KT3BZ-PKWCP-YTTDB-VFDGS-R5EF5-ASB67',
       'RLDBZ-6CP6P-WSJDZ-LUQNM-LMDR3-SEBMY']


#
def get_list(attr, json, default=None):
    for item in attr:
        for x in _get_list(item, json, default):
            yield x


def _get_list(attr, json, default=None):
    if type(attr) is tuple:
        for item in attr[1]:
            for x in _get_list(item, json[attr[0]], default):
                yield str(x)
    else:
        try:
            yield json[attr]
        except:
            yield default


#
conn2 = psycopg2.connect("host=localhost port=5439 user=postgres dbname=postgres")
cur2 = conn2.cursor()
api_url = u'http://apis.map.qq.com/ws/place/v1/search?' \
          'boundary=region(北京,0)' \
          '&keyword={0}' \
          '&page_size=20' \
          '&page_index={1}' \
          '&key={2}'


class TencentSpider(Spider):
    name = 'T'
    index = 0
    ind=0
    def start_requests(self):
        cur2.execute("select name from poi.gaodepois ")
        keywords = cur2.fetchall()
        f = open('1.txt','r+')
        start_index=int(f.read())
        f.close()
        se=set()
        for i in range(start_index, keywords.__len__()):
            k = keywords[i]
            if i % 100 == 0:
                print (i / 100)
            self.ind = i
            x = k[0][-4:]
            if x in se:
                continue
            else:
                se.add(x)
            yield Request(api_url.format(x, 1, key[self.index]),
                          self.parse, meta={'page_index': 1, 'keyword': x, 'index': self.index})

    def parse(self, response):
        j = json.loads(response.body.decode('utf-8'))
        meta = response.meta

        if j['status'] == 0:
            for x in j['data']:
                boudary = ''
                panoid = ''
                heading = ''
                pitch = ''
                zoom = ''
                if u'boundary' in x:
                    boudary = ' '.join(x['boundary'])
                if u'pano' in x:
                    if u'id' in x['pano']:
                        panoid = x['pano']['id']
                        heading = str(x['pano']['heading'])
                        pitch = str(x['pano']['pitch'])
                        zoom = str(x['pano']['zoom'])
                cur2.execute(
                    "insert into poi.bj_tencentpois values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    "on conflict do nothing", (
                        x['id'],
                        x['title'],
                        x['address'],
                        x['tel'],
                        x['category'],
                        str(x['type']),
                        str(x['location']['lat']),
                        str(x['location']['lng']),
                        x['ad_info']['adcode'],
                        boudary,
                        panoid,
                        heading,
                        pitch,
                        zoom,
                        0
                    ))
                conn2.commit()
            if response.meta['page_index'] * 20 < j['count']:
                meta['page_index'] += 1
                yield Request(api_url.format(meta['keyword'], meta['page_index'], key[meta['index']]),
                              self.parse, meta=meta)
        elif j['status'] == 121:
            meta['index'] += 1
            print()
            if meta['index']>=10:
                f = open('1.txt', 'w+')
                f.write(str(self.ind))
                f.flush()
                raise CloseSpider('key 用完了耶')
            self.index = response.meta['index']
            yield Request(api_url.format(meta['keyword'], meta['page_index'], key[meta['index']]),
                          self.parse, meta=meta)
        else:
            print(j)
