# -*- coding: utf-8 -*-

import time
import numpy as np
from datetime import datetime, timedelta

import simplejson as json
import itertools
import scrapy
from exchange_rate_scraper.items import XeItem
from exchange_rate_scraper import currency

from twisted.enterprise import adbapi
import MySQLdb
import MySQLdb.cursors
import pdb
class OandaSpider(scrapy.Spider):
    name = 'xe'
    allowed_domains = ['xe.com']
  
    url_tpl = "http://www.xe.com/currencytables/?from=%(from_currency)s&date=%(date)s"
    custom_settings = {
        'ITEM_PIPELINES':{
            'exchange_rate_scraper.pipelines.MySQLStoreXePipeline': 300,
        },
    }
    urls = []
    meta = []
    dates = []
    def __init__(self, *args, **kwargs):
        super(OandaSpider, self).__init__(*args, **kwargs)

        self.dbpool = kwargs['dbpool']
        from_date = kwargs['from_date']
        from_currency = kwargs['from_currency']
       
            
        self.currency_helper = currency.Currency()
        currency_pairs = self.currency_helper.get_pairs(from_currency)
        
        date_fmt = '%Y-%m-%d'
        from_date_1 = datetime.strptime(from_date, date_fmt)
        from_date
        for i in xrange((datetime.now() - from_date_1).days):

            self.dates.append((from_date_1 + timedelta(days=i)).strftime(date_fmt))
            
        
        d = self.dbpool.runInteraction(self.init_currency, currency_pairs)
        
        for date in self.dates:    
            params = {
                'from_currency': from_currency,
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

    def init_currency(self, conn, currency_pairs):

        for fc, tc in currency_pairs:
            sql = "INSERT IGNORE INTO xe(from_currency, to_currency, date, from_to_value, to_from_value) values"
            params = []
            for date in self.dates:
                params.append("('%s', '%s', '%s', %s, %s)"% (fc, tc, date, 0, 0))
            sql += ','.join(params)
            print '\n============\n'
            print sql
            print '\n============\n'
            conn.execute(sql) 

    def start_requests(self):
        for i in range(len(self.urls)):
            yield scrapy.Request(self.urls[i], meta=self.meta[i])

        #return [scrapy.Request(self.url, headers=self.headers, meta=meta)]

    def parse(self, response):
        a = response.xpath('//table[@id="historicalRateTbl"]/tbody/tr')
        for b in a:
            c=b.re('<tr><td.*><a.*>(.*)<\/a><\/td><td*.>(.*?)<\/td><td.*>(\d+\.\d+)<\/td><td.*>(\d+\.\d+)<\/td>.*<\/tr>')
            item = XeItem()
            item['from_currency'] = response.meta['from_currency']
            item['to_currency'] = c[0]
            item['date'] = response.meta['date']
            item['from_to_value'] = c[2]
            item['to_from_value'] = c[3]
            yield item
