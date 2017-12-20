from scrapy import *
from scrapy.exceptions import CloseSpider
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
import json, psycopg2, re
# 15120954
import sys


key = ['CGSBZ-VJDK6-DQZSW-MFE7A-MLN4E-K5FQ3',
       'ZHXBZ-IGXWW-QCXRZ-RHKET-AKJL5-IXF3P',
       'GHQBZ-2TTW6-H3BSF-MKEDU-ISHBJ-LRB2J',
       'VC4BZ-3FO3I-IPVGB-5UXHC-IYJSE-T4B3L',
       'KUIBZ-YU66I-KHAGH-5TF77-LOQSO-NHFXN',
       'XATBZ-VQ5CO-HKPWI-SMCF5-JOPGE-JLFOS',
       '6O4BZ-H5R3G-362QG-IKR4C-AZDDH-4GFRJ',
       'DUWBZ-LDYCW-ETLRF-OP7ZE-KHJQS-ILBN5',
       '7ZKBZ-77NK6-I3HSB-M7RRD-T4JBF-CWFI4',
       'RZWBZ-KN5KU-4UAVM-2W2MY-7ECN2-K2FMZ']


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



api_url = 'http://apis.map.qq.com/ws/streetview/v1/getpano?id=10011504120403141232200&'\
          'radius=100&key={'



class PanoSpider(Spider):
    name = 'Pano'
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
            x = k[0][:3]
            if x in se:
                continue
            else:
                se.add(x)
            yield Request(api_url.format(x, 1, key[self.index]),
                          self.parse, meta={'page_index': 1, 'keyword': x, 'index': self.index})

    def parse(self, response):
        j = json.loads(response.body)
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
                    "insert into poi.tencentpois values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    " on conflict do nothing", (
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
                        zoom
                    ))
                conn2.commit()
            if response.meta['page_index'] * 20 < j['count']:
                meta['page_index'] += 1
                yield Request(api_url.format(meta['keyword'], meta['page_index'], key[meta['index']]),
                              self.parse, meta=meta)
        elif j['status'] == 121:
            meta['index'] += 1
            if meta['index']>=10:
                f = open('1.txt', 'w+')
                f.write(str(self.ind))
                f.flush()
                raise CloseSpider('key 用完了耶')
            self.index = response.meta['index']
            yield Request(api_url.format(meta['keyword'], meta['page_index'], key[meta['index']]),
                          self.parse, meta=meta)
        else:
            print(j['status'])
