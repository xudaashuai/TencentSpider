# coding:utf-8
from scrapy import *
from scrapy.exceptions import CloseSpider
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
import json, psycopg2, re
# 15120954
import sys


key = ['KRhxgE8XUjk02QQ7H8Grp4SIBW0S3KCj']


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
api_url = 'http://api.map.baidu.com/place/v2/search?query={0}&page_size=20&page_num={1}&scope=1&region=武汉&output=json&ak={2}'


class BaiduSpider(Spider):
    name = 'B'
    index = 0
    ind=0
    def start_requests(self):
        cur2.execute("select name from poi.gaodepois ")
        keywords = cur2.fetchall()
        f = open('b1.txt','r+')
        start_index=int(f.read())
        f.close()
        se=set()
        for i in range(start_index, keywords.__len__()):
            k = keywords[i]
            if i % 100 == 0:
                print (i / 100)
            self.ind = i
            x = k[0][:4]
            if x in se:
                continue
            else:
                se.add(x)
            yield Request(api_url.format(x, 1, key[self.index]),
                          self.parse, meta={'page_index': 1, 'keyword': x, 'index': self.index})

    def parse(self, response):
        print(response.url)
        j = json.loads(response.body)
        meta = response.meta
        if j['status'] == 0:
            for x in j['results']:
                if 'street_id' not in x:
                    continue
                cur2.execute(
                    "insert into poi.baidupic values (%s,%s,%s)"
                    " on conflict do nothing", (
                        x['uid'],x['street_id'],0
                    ))
                conn2.commit()
            if response.meta['page_index'] * 20 < j['total']:
                meta['page_index'] += 1
                yield Request(api_url.format(meta['keyword'], meta['page_index'], key[meta['index']]),
                              self.parse, meta=meta)
        elif j['status'] == 302:
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
