# -*- coding: utf-8 -*-

import itertools

ALL_CURRENCY = (
    'AED', 'KWD',
    #'GBP', 'USD', 'EUR', 'ALL', 'AOA', 'ARS', 'AUD', 
    #'BHD', 'BDT', 'BYR', 'BGN', 'BOB', 'BRL', 'BND', 
    #'CAD', 'CLP', 'CNY', 'COP', 'CDF', 'CRC', 'HRK', 'CZK', 
    #'DKK', 'EGP', 'FJD', 'HKD', 'HUF', 'ISK', 'INR', 'IDR', 'ILS', 'JPY', 
    #'KRW', 'KWD', 'LVL', 'LTL', 'MYR', 'MXN', 'NZD', 'NOK', 'PKR', 'PHP', 'PLN', 
    #'RUB', 'SAR', 'SGD', 'ZAR', 'SEK', 'CHF', 'THB', 'TRY', 'AED', 'TWD', 'UAH'
)

class Currency(object):
    
    @classmethod
    def get_pairs(self, from_currency=None):
        if from_currency:
            # 指定from_currency
            assert (from_currency in ALL_CURRENCY)
            
            for tc in ALL_CURRENCY:
                if from_currency != tc:
                    yield (from_currency, tc)
        else:
            # 笛卡尔积
            for fc, tc in itertools.product(ALL_CURRENCY, ALL_CURRENCY):
                if fc != tc:
                    yield (fc, tc)


if __name__ == '__main__':
    for fc, tc in Currency().get_pairs(from_currency='CNY'):
        print fc, tc
