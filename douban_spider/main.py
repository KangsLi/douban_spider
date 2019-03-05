#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time : 2019\1\20 0020 21:18 
# @Author : fz 
# @File : main.py
import threading
import time

from scrapy.cmdline import execute
from tools.proxy_ip import GetIp

import sys
import os

#
# t = threading.Thread(target=GetIp.start_get_ips,name="getIps")
# t.start()
# time.sleep(10)


sys.path.append(os.path.dirname(os.path.abspath(__file__)))
execute(["scrapy", "crawl", "movie_comment_spider"])

