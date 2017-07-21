# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from datetime import datetime
from exchange_rate_scraper import settings
from scrapy.exceptions import DropItem
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
            host=settings['MYSQL']['HOST'],
            port=settings['MYSQL']['PORT'],
            db=settings['MYSQL']['DBNAME'],
            user=settings['MYSQL']['USER'],
            passwd=settings['MYSQL']['PWD'],
            charset='utf8',
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=True
        )
        dbpool = adbapi.ConnectionPool('MySQLdb', **dbargs)
        
        return cls(dbpool)

    def process_item(self, item, spider):
        # 忽略比目标开始时间早的数据
        date_fmt = '%Y-%m-%d'
        if datetime.strptime(item['date'], date_fmt) < datetime.strptime(spider.meta[0]['from_date'], date_fmt):
            raise DropItem("Date too old %s" % item)
        
        d = self.dbpool.runInteraction(self._do_upinsert, item, spider)
        d.addErrback(self._handle_error, item, spider)
        d.addBoth(lambda _: item)
        return d
        
    def _do_upinsert(self, conn, item, spider):
        #print '\n=============\n'
        #print """
        #    UPDATE reuters SET high=%s, low=%s, open=%s, close=%s WHERE from_currency = '%s' AND to_currency = '%s' AND date = '%s'
        #    """% (item['high_rate'], item['low_rate'], item['open_rate'], item['close_rate'], item['from_currency'], item['to_currency'], item['date'])
        #print '\n=============\n'
        
        # 奇怪，用逗号就不行。。。
        conn.execute(
            #"""
            #UPDATE reuters SET high=%s, low=%s, open=%s, close=%s WHERE from_currency = '%s' AND to_currency = '%s' AND date = '%s'
            #""", (item['high_rate'], item['low_rate'], item['open_rate'], item['close_rate'], item['from_currency'], item['to_currency'], item['date'])

            """
            UPDATE reuters SET high=%s, low=%s, open=%s, close=%s WHERE from_currency = '%s' AND to_currency = '%s' AND date = '%s'
            """% (item['high_rate'], item['low_rate'], item['open_rate'], item['close_rate'], item['from_currency'], item['to_currency'], item['date'])
        )


    def _handle_error(self, failue, item, spider):
        log.err(failue)


class MySQLStoreOandaPipeline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool
    
    @classmethod
    def from_settings(cls, settings):
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
        dbpool = adbapi.ConnectionPool('MySQLdb', **dbargs)
        
        return cls(dbpool)

    def process_item(self, item, spider):
        # 忽略比目标开始时间早的数据
        # date_fmt = '%Y-%m-%d'
        # if datetime.strptime(item['date'], date_fmt) < datetime.strptime(spider.meta[0]['from_date'], date_fmt):
        #     raise DropItem("Date too old %s" % item)
        
        d = self.dbpool.runInteraction(self._do_upinsert, item, spider)
        d.addErrback(self._handle_error, item, spider)
        d.addBoth(lambda _: item)
        return d
        
    def _do_upinsert(self, conn, item, spider):
        #print '\n=============\n'
        #print """
        #    UPDATE reuters SET high=%s, low=%s, open=%s, close=%s WHERE from_currency = '%s' AND to_currency = '%s' AND date = '%s'
        #    """% (item['high_rate'], item['low_rate'], item['open_rate'], item['close_rate'], item['from_currency'], item['to_currency'], item['date'])
        #print '\n=============\n'
        print '\n=============111111111\n'
        # 奇怪，用逗号就不行。。。
        conn.execute(
            #"""
            #UPDATE reuters SET high=%s, low=%s, open=%s, close=%s WHERE from_currency = '%s' AND to_currency = '%s' AND date = '%s'
            #""", (item['high_rate'], item['low_rate'], item['open_rate'], item['close_rate'], item['from_currency'], item['to_currency'], item['date'])

            """
            UPDATE oanda SET value=%s WHERE from_currency = '%s' AND to_currency = '%s' AND date = '%s'
            """% (item['value'], item['from_currency'], item['to_currency'], item['date'])
        )


    def _handle_error(self, failue, item, spider):
        log.err(failue)


class MySQLStoreXePipeline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool
    
    @classmethod
    def from_settings(cls, settings):
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
        dbpool = adbapi.ConnectionPool('MySQLdb', **dbargs)
        
        return cls(dbpool)

    def process_item(self, item, spider):
        # 忽略比目标开始时间早的数据
        # date_fmt = '%Y-%m-%d'
        # if datetime.strptime(item['date'], date_fmt) < datetime.strptime(spider.meta[0]['date'], date_fmt):
        #     raise DropItem("Date too old %s" % item)
        
        d = self.dbpool.runInteraction(self._do_upinsert, item, spider)
        d.addErrback(self._handle_error, item, spider)
        d.addBoth(lambda _: item)
        return d
        
    def _do_upinsert(self, conn, item, spider):
        #print '\n=============\n'
        #print """
        #    UPDATE reuters SET high=%s, low=%s, open=%s, close=%s WHERE from_currency = '%s' AND to_currency = '%s' AND date = '%s'
        #    """% (item['high_rate'], item['low_rate'], item['open_rate'], item['close_rate'], item['from_currency'], item['to_currency'], item['date'])
        #print '\n=============\n'
        print '\n=============111111111\n'
        # 奇怪，用逗号就不行。。。
        conn.execute(
            #"""
            #UPDATE reuters SET high=%s, low=%s, open=%s, close=%s WHERE from_currency = '%s' AND to_currency = '%s' AND date = '%s'
            #""", (item['high_rate'], item['low_rate'], item['open_rate'], item['close_rate'], item['from_currency'], item['to_currency'], item['date'])

            """
            UPDATE xe SET from_to_value=%s, to_from_value=%s WHERE from_currency = '%s' AND to_currency = '%s' AND date = '%s'
            """% (item['from_to_value'], item['to_from_value'], item['from_currency'], item['to_currency'], item['date'])
        )


    def _handle_error(self, failue, item, spider):
        log.err(failue)
