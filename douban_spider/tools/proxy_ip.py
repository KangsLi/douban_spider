#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time : 2019\1\28 0028 14:16 
# @Author : fz 
# @File : proxy_ip.py

import requests
import json
import time

from utils.DBUtil import MysqlUtil


class GetIp(object):

    @classmethod
    def start_get_ips(self):
        while True:
            url = "https://proxy.horocn.com/api/proxies?order_id=NABN1625612321117848&num=10&format=json&line_separator=win&can_repeat=yes"
            try:
                response = requests.get(url, timeout=1)
            except Exception:
                print(Exception)
            jsons = json.loads(response.text)
            for ip_proxy in jsons:
                ip = ip_proxy['host']
                port = ip_proxy['port']
                url = "http://{0}:{1}".format(ip, port)
                insert_sql = "insert into ip_proxy(url) values('{0}')".format(url)
                mysql = MysqlUtil()
                mysql.update(insert_sql)
                mysql.dispose()
            time.sleep(10)

    def delete_ip(self, url):
        delete_sql = 'delete from ip_proxy where url = "{0}"'.format(url)
        mysql = MysqlUtil()
        mysql.delete(delete_sql)
        mysql.dispose()
        return True

    def get_random_ip(self):
        sql = """
            SELECT url from ip_proxy
            ORDER BY RAND()
            LIMIT 1
        """
        mysql = MysqlUtil()
        result = mysql.getOne(sql)
        if result:
            url = result[0]
            return url
        mysql.dispose()
