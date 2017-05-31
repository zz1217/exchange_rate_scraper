# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ExchangeRateScraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class ReutersItem(scrapy.Item):
    from_currency = scrapy.Field()
    to_currency = scrapy.Field()
    date = scrapy.Field()
    open_rate = scrapy.Field()
    close_rate = scrapy.Field()
    high_rate = scrapy.Field()
    low_rate = scrapy.Field()
