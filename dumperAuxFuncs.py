__author__ = 'ggarrido'

import datetime
import re

validTimeRE = re.compile(ur'\d\d:\d\d')

def convertStrBoolean(value, col_attrs=None):
    if not value or not (isinstance(value, str) or isinstance(value, int)):
        return False
    return False if value == 0 or int(value) == 0 else True

def defaultDate(value, format, defaultValue, nullable):
    if value is None or value[:4] == '0000':
        return None if nullable else '1900-01-01'
    return value

def notNullableDate(value, col_attrs=None):
    nullable = col_attrs['nullable'] if col_attrs else False
    format, defaultValue = "%d%m%Y", "01011900"
    return defaultDate(value, format, defaultValue, nullable)


def notNullableDatetime(value, col_attrs=None):
    nullable = col_attrs['nullable'] if col_attrs else False
    format, defaultValue = "%d%m%Y %H:%M:%S", "01011900 00:00:00"
    return defaultDate(value, format, defaultValue, nullable)

def refToNullable(value, col_attrs=None):
    nullable = col_attrs['nullable'] if col_attrs else False
    if (value == 0 or value == '0') and nullable: return None
    return value

def makeItEmpty(value, col_attrs=None):
    nullable = col_attrs['nullable'] if col_attrs else False
    return None if nullable else ''

def makeItTime(value, col_attrs=None):
    nullable = col_attrs['nullable'] if col_attrs else False
    if value is not None and re.match(validTimeRE, value): return value
    return None if nullable else '00:00'
