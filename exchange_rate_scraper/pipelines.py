#! /bi/anaconda2/bin/python
# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

#from scrapy.conf import settings
from exchange_rate_scraper import settings
from twisted.enterprise import adbapi
import MySQLdb
import MySQLdb.cursors


class ExchangeRateScraperPipeline(object):
    def process_item(self, item, spider):
        return item


class MySQLStoreReutersPipeline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool
    
    @classmethod
    def from_settings(cls, settings):
        dbargs = dict(
            host=settings['MYSQL_HOST'],
            port=settings['MYSQL_PORT'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PWD'],
            charset='utf8',
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=True
        )
        dbpool = adbapi.ConnectionPool('MySQLdb', **dbargs)
        
        return cls(dbpool)

    def process_item(self, item, spider):
        d = self.dbpool.runInteraction(self._do_upinsert, item, spider)
        d.addErrback(self._handle_error, item, spider)
        d.addBoth(lambda _: item)
        return d
        
    def _do_upinsert(self, conn, item, spider):
        conn.execute(
            """
            INSERT IGNORE INTO reuters(from_currency, to_currency, date, high, low, open, close) values(%s, %s, %s, %s, %s, %s, %s)
            """, (item['from_currency'], item['to_currency'], item['date'], item['high_rate'], item['low_rate'], item['open_rate'], item['close_rate'])
        )

    def _handle_error(self, failue, item, spider):
        log.err(failue)
