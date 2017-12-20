# coding:utf-8
from scrapy import *
from scrapy.exceptions import CloseSpider
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
import json, psycopg2, re
# 15120954
import sys

conn = psycopg2.connect("host=localhost port=5439 user=postgres dbname=postgres")
cur = conn.cursor()
key = ['KRhxgE8XUjk02QQ7H8Grp4SIBW0S3KCj']

api_url = 'http://api.map.baidu.com/panorama/v2?ak={1}&width=1024&height=512&panoid={0}'


class PictureSpider(Spider):
    name = 'P'
    index = 0
    ind = 0

    def start_requests(self):
        cur.execute("SELECT id,panoid FROM poi.baidupic WHERE pano = 0")
        ids = cur.fetchall()
        for i in range(len(ids)):
            panoid = ids[i][1]
            if i % 100 == 0:
                print(i / 100)
            yield Request(api_url.format(panoid, key[self.index]),
                          self.parse,
                          meta={'id': panoid,
                                'index': self.index})

    def parse(self, response):
        meta = response.meta
        if response.body.__len__() < 2000:
            j = json.loads(response.body.decode('utf-8'))
            print(j)
            if j['status'] == '302':
                meta['index'] += 1
                self.index = meta['index']
                print(self.index, 'key 结束啦')

                if meta['index'] >= key.__len__():
                    raise CloseSpider('key 用完了耶')
            else:
                print(self.index, 'pano 有问题')
                cur.execute("UPDATE poi.baidupic SET pano=-1 WHERE panoid = %s", (meta['id'],))
                conn.commit()
                print(j)
        else:
            f2 = open('F:/百度街景图片/' + '{0}'.format(meta['id']) + '.jpg', 'wb+')
            f2.write(response.body)
            f2.flush()
            f2.close()
            self.ind += 1
            if self.ind % 100 == 0:
                print(self.ind)
            cur.execute("UPDATE poi.baidupic SET pano=1 WHERE panoid = %s", (meta['id'],))
            conn.commit()
