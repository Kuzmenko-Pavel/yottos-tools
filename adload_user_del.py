# -*- coding: utf-8 -*-
import sys

import os
import time

project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
os.environ['PYTHON_EGG_CACHE'] = '/usr/lib/python2.7/dist-packages'
import datetime
import sys
from pymongo import MongoClient
from collections import defaultdict
import urlparse
import urllib
import pymssql
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment
import StringIO
import ftplib
import requests


sys.stdout = sys.stderr



def mssql_connection_adload():
    """

    Returns:

    """
    pymssql.set_max_connections(450)
    conn = pymssql.connect(host='srv-1.yottos.com',
                           user='web',
                           password='odif8duuisdofj',
                           database='1gb_YottosAdLoad',
                           as_dict=True,
                           charset='cp1251')
    conn.autocommit(True)
    return conn

accounts = defaultdict(lambda: False)
connection_adload = mssql_connection_adload()
cursor = connection_adload.cursor()
cursor.execute('''SELECT [UserID], [Login] FROM [1gb_YottosAdLoad].[dbo].[Users]''')
for row in cursor:
    accounts[row['Login'].decode('utf-8')] = str(row['UserID'])
cursor.close()

with open('del_user') as f:
    for login in f:
        guid = accounts[login.strip()]
        if guid:
            print(login, guid)
            cursor = connection_adload.cursor()
            try:
                cursor.execute('''exec UserDeleteAll @UserID=%s ''', guid)
            except Exception as e:
                print(e)
            cursor.close()
