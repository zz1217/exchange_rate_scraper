# -*- coding: utf-8 -*-

import numpy as np
import simplejson as json
import itertools
import scrapy
from exchange_rate_scraper.items import ReutersItem
from scrapy.selector import Selector


class ReutersSpider(scrapy.Spider):
    name = 'reuters'
    allowed_domains = ['reuters.com']
    
    all_currency = ('GBP', 'USD', 'EUR', 'ALL', 'AOA', 'ARS', 'AUD', 
            'BHD', 'BDT', 'BYR', 'BGN', 'BOB', 'BRL', 'BND', 
            'CAD', 'CLP', 'CNY', 'COP', 'CDF', 'CRC', 'HRK', 'CZK', 
            'DKK', 'EGP', 'FJD', 'HKD', 'HUF', 'ISK', 'INR', 'IDR', 'ILS', 'JPY', 
            'KRW', 'KWD', 'LVL', 'LTL', 'MYR', 'MXN', 'NZD', 'NOK', 'PKR', 'PHP', 'PLN', 
            'RUB', 'SAR', 'SGD', 'ZAR', 'SEK', 'CHF', 'THB', 'TRY', 'AED', 'TWD', 'UAH')

    url_tpl = "http://www.reuters.wallst.com/reuters/enhancements/CN/interactiveChart/currencychart.asp?currencies=%(from_currency)s,%(to_currency)s"

    urls = []
    meta = []

    def __init__(self, date=None, *args, **kwargs):
        super(ReutersSpider, self).__init__(*args, **kwargs)
        
        '''
        for i in itertools.product(self.all_currency, self.all_currency):
            params = {
                'from_currency': i[0],
                'to_currency': i[1],
                'date': date
            }
            self.meta.append(params)
            self.urls.append(self.url_tpl% params)
        '''
        params = {
            'from_currency': 'CNY',
            'to_currency': 'AUD',
        }
        self.meta.append(params)
        self.urls.append(self.url_tpl% params)

    def start_requests(self):
        for i in range(len(self.urls)):
            yield scrapy.Request(self.urls[i], meta=self.meta[i])

        #return [scrapy.Request(self.url, headers=self.headers, meta=meta)]

    def parse(self, response):
        print '>>>>>response type is %s'% type(response)
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
