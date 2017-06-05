# -*- coding: utf-8 -*-

import numpy as np
from datetime import datetime, timedelta

import scrapy
from scrapy.selector import Selector
from exchange_rate_scraper.items import ReutersItem
from exchange_rate_scraper import currency

from twisted.enterprise import adbapi
import MySQLdb
import MySQLdb.cursors


class ReutersSpider(scrapy.Spider):
    name = 'reuters'
    allowed_domains = ['reuters.com']
    
    url_tpl = "http://www.reuters.wallst.com/reuters/enhancements/CN/interactiveChart/currencychart.asp?currencies=%(from_currency)s,%(to_currency)s"

    urls = []
    meta = []

    custom_settings = {
        'ITEM_PIPELINES':{
            'exchange_rate_scraper.pipelines.MySQLStoreReutersPipeline': 300,
        },
    }

    def __init__(self, *args, **kwargs):
        super(ReutersSpider, self).__init__(*args, **kwargs)
        
        self.dbpool = kwargs['dbpool']
        from_date = kwargs['from_date']

        self.currency_helper = currency.Currency()
        currency_pairs = self.currency_helper.get_pairs()

        # pre-insert all currency data in database
        d = self.dbpool.runInteraction(self.init_currency, currency_pairs, from_date)
        
        for fc, tc in self.currency_helper.get_pairs():
            params = {
                'from_currency': fc,
                'to_currency': tc,
                'from_date': from_date
            }
            self.meta.append(params)
            self.urls.append(self.url_tpl% params)
            
            # for debug
            #break
        
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
    
    def init_currency(self, conn, currency_pairs, from_date):
        dates = []
        date_fmt = '%Y-%m-%d'
        from_date = datetime.strptime(from_date, date_fmt)
        from_date
        for i in xrange((datetime.now() - from_date).days):
            dates.append((from_date + timedelta(days=i)).strftime(date_fmt))
        
        for fc, tc in currency_pairs:
            sql = "INSERT IGNORE INTO reuters(from_currency, to_currency, date, high, low, open, close) values"
            params = []
            for date in dates:
                params.append("('%s', '%s', '%s', %s, %s, %s, %s)"% (fc, tc, date, 0, 0, 0, 0))
            
            sql += ','.join(params)
                
            #print '\n============\n'
            #print sql
            #print '\n============\n'

            conn.execute(sql)
        

    def start_requests(self):
        for i in range(len(self.urls)):
            yield scrapy.Request(self.urls[i], meta=self.meta[i])

    def parse(self, response):
        resp = Selector(response=response, type="xml").re('"open":"[^\d]+([\d\.]+)","close":"([\d\.]+)","high":"([\d\.]+)","low":"([\d\.]+)","date":"(\d{4}-\d{2}-\d{2})"')
        
        for i in np.array(resp).reshape(len(resp)/5, 5):
            open_rate, close_rate, high_rate, low_rate, date = i
            item = ReutersItem()
            item['from_currency'] = response.meta['from_currency']
            item['to_currency'] = response.meta['to_currency']
            item['date'] = date
            item['open_rate'] = open_rate
            item['close_rate'] = close_rate
            item['high_rate'] = high_rate
            item['low_rate'] = low_rate
            yield item
