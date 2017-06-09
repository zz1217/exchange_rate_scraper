# -*- coding: utf-8 -*-

import time
import simplejson as json
import itertools
import scrapy
from exchange_rate_scraper.items import OandaItem
from exchange_rate_scraper import currency

from twisted.enterprise import adbapi
import MySQLdb
import MySQLdb.cursors

class OandaSpider(scrapy.Spider):
    name = 'oanda'
    allowed_domains = ['oanda.com']
  

    url_tpl = "https://www.oanda.com/lang/cns/currency/converter/update?base_currency_0=%(to_currency)s&quote_currency=%(from_currency)s&end_date=%(date)s&view=details&id=4&action=D&"

    urls = []
    meta = []

    custom_settings = {
        'ITEM_PIPELINES':{
            'exchange_rate_scraper.pipelines.MySQLStoreOandaPipeline': 300,
        },
    }

    headers = {
        ':authority':'www.oanda.com',
        ':method':'GET',
        ':scheme':'https',
        'accept':'text/javascript, text/html, application/xml, text/xml, */\*',
        'accept-encoding':'gzip, deflate, sdch',
        'accept-language:':'zh-CN,zh;q=0.8',
        'referer':'https://www.oanda.com/lang/cns/currency/converter/',
        'user-agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, ke Gecko) Chrome/47.0.2526.80 Safari/537.36',
        'x-prototype-version':'1.7',
        'x-requested-with':'XMLHttpRequest',
    }

    def __init__(self, date=None, *args, **kwargs):
        super(OandaSpider, self).__init__(*args, **kwargs)
        self.dbpool = kwargs['dbpool']
        self.currency_helper = currency.Currency()
        currency_pairs = self.currency_helper.get_pairs()

        if date==None:
                date = time.strftime('%Y-%m-%d',time.localtime(time.time() - 24*60*60) )

        d = self.dbpool.runInteraction(self.init_currency, currency_pairs, date)

        for fc, tc in self.currency_helper.get_pairs():
            params = {
                'from_currency': fc,
                'to_currency': tc,
                'date': date
            }
            self.meta.append(params)
            self.urls.append(self.url_tpl% params)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        settings = crawler.settings
        dbargs = dict(
            host=settings['MYSQL']['HOST'],
            port=settings['MYSQL']['PORT'],
            db=settings['MYSQL']['DBNAME'],
            user=settings['MYSQL']['USER'],
            passwd=settings['MYSQL']['PWD'],
            charset='utf8',
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=True
        )
        kwargs['dbpool'] = adbapi.ConnectionPool('MySQLdb', **dbargs)
        return cls(*args, **kwargs)

    def init_currency(self, conn, currency_pairs, date):
        for fc, tc in currency_pairs:
            sql = "INSERT IGNORE INTO oanda(from_currency, to_currency, date, value) values"
            sql += "('%s', '%s', '%s', %s)"% (fc, tc, date, 0)
            # print '\n============\n'
            # print sql
            # print '\n============\n'
            conn.execute(sql) 

    def start_requests(self):
        for i in range(len(self.urls)):
            yield scrapy.Request(self.urls[i], headers=self.headers, meta=self.meta[i])

        #return [scrapy.Request(self.url, headers=self.headers, meta=meta)]

    def parse(self, response):
        d = json.loads(response.body_as_unicode())

        item = OandaItem()
        item['from_currency'] = response.meta['from_currency']
        item['to_currency'] = response.meta['to_currency']
        item['date'] = response.meta['date']
        item['value'] = d['data']['bid_ask_data']['bid']
        yield item