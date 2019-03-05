#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time : 2019\1\20 0020 21:19 
# @Author : fz 
# @File : movie_item_spider.py

import scrapy
import json

from urllib import parse as urlparse
from scrapy.loader import ItemLoader

from DoubanSpider.items import DoubanMovieItem, MovieItemLoader


class MovieItemSpider(scrapy.Spider):
    name = 'movie_item_spider'
    allowed_domains = ['movie.douban.com']
    start_urls = ['https://movie.douban.com/subject/1292489/']

    formdata = {
        'name': '3041354160@qq.com',
        'password': 'fzyt9598..',
        'remember': 'true',
    }

    def start_requests(self):
        for i in range(0, 250, 25):
            first_url = 'https://movie.douban.com/top250?start={0}&filter='.format(i)
            yield scrapy.Request(
                first_url,
                callback=self.parse_top250,
            )

    def parse_top250(self,response):
        urls = response.xpath('//li/div[@class="item"]/div[@class="pic"]/a/@href').extract()
        for url in urls:
            yield scrapy.Request(
                url,
                callback=self.parse_item,
            )


    def start_requests_item(self):
        genres = ['剧情', '喜剧', '动作',
                  '爱情', '科幻', '动画',
                  '悬疑',
                  # '惊悚', '恐怖', '犯罪',
                  # '同性', '音乐', '歌舞',
                  # '传记', '历史', '战争',
                  # '西部', '奇幻', '冒险',
                  # '灾难', '武侠', '情色'
                  ]

        for gen in genres:
            for count in range(0, 9800, 20):
                fir_url = 'https://movie.douban.com/j/new_search_subjects?sort=U&range=0,10&tags=电影&start={0}&genres={1}'.format(
                    count, gen)
                yield scrapy.Request(
                    fir_url,
                    callback=self.parse_json,
                )

    def start_requests_update(self):
        from utils.DBUtil import MysqlUtil

        mysql = MysqlUtil()

        sql = 'select * from movie_item where movie_title = "{0}"'.format("无")

        result = mysql.getAll(sql=sql)

        for re in result:
            yield scrapy.Request(url=re[1], callback=self.parse_item)

    def login(self, response):
        print('Loging...')
        link = response.xpath('.//img[@id="captcha_image"]/@src').extract()
        if len(link) > 0:
            print("此时有验证码")
            print('Copy the link:')
            print(link)
            captcha_solution = input('captcha-solution:')
            captcha_id = urlparse.parse_qs(urlparse.urlparse(link[0]).query, True)['id']
            self.formdata['captcha-solution'] = captcha_solution
            self.formdata['captcha-id'] = captcha_id
        return [scrapy.FormRequest(
            url='https://accounts.douban.com/j/mobile/login/basic',
            formdata=self.formdata,
            callback=self.after_login
        )]

    def after_login(self, response):

        # 验证服务器的返回数据判断是否成功
        text_json = json.loads(response.text)
        if "message" in text_json and text_json["message"] == "success":
            for count in range(0, 9980, 20):
                fir_url = 'https://movie.douban.com/j/new_search_subjects?sort=T&range=0,10&tags=&start={}'.format(
                    count)
                yield scrapy.Request(
                    fir_url,
                    callback=self.parse_json,
                )

    def parse_json(self, response):
        infos = json.loads(response.body.decode('utf-8'))
        datas = infos['data']
        for data in datas:
            url = data['url']
            if url:
                yield scrapy.Request(url, callback=self.parse_item)

    def parse_item(self, response):
        item_loader = MovieItemLoader(item=DoubanMovieItem(), response=response)
        # 评分百分比数处理
        rating_per_stars = response.xpath('.//span[@class="rating_per"]/text()').extract()
        # 相关数字处理
        related_num = response.xpath('.//div[@class="mod-hd"]/h2/span/a/text()').extract()
        rating_num = response.xpath('.//strong[@class="ll rating_num"]/text()').extract()
        vote_num = response.xpath('.//span[@property="v:votes"]/text()').extract()
        # info处理
        info = response.xpath('.//div[@id="info"]/text()').extract()
        info = [p for p in info if (p.strip() != '') & (p.strip() != '/')]
        # 时间处理
        release_date = response.xpath('//*[@id="info"]/span[@property="v:initialReleaseDate"]/text()').extract()
        # 电影的唯一ID
        item_loader.add_value('movie_id', response.url)
        # 电影的url
        item_loader.add_value('url', response.url)
        movie_title = response.xpath('.//h1/span[@property="v:itemreviewed"]/text()').extract()
        if movie_title:
            # 电影名字
            item_loader.add_value('movie_title', movie_title)
        else:
            # 电影名字
            item_loader.add_value('movie_title', '无')
        if release_date:
            # 电影上映日期
            item_loader.add_xpath('release_date', '//*[@id="info"]/span[@property="v:initialReleaseDate"]/text()')
        else:
            item_loader.add_value('release_date', '0001-01-01')
        # 电影导演
        directedBy = response.xpath('.//a[@rel="v:directedBy"]/text()').extract()
        if directedBy:
            item_loader.add_value('directedBy', directedBy)
        else:
            item_loader.add_value('directedBy', '无')
        # 电影编剧
        screenwriter = response.xpath('//*[@id="info"]/span[2]/span[2]/a/text()').extract()
        if screenwriter:
            item_loader.add_value('screenwriter', screenwriter)
        else:
            item_loader.add_value('screenwriter', '无')
        # 电影主演
        starring = response.xpath('.//a[@rel="v:starring"]/text()').extract()
        if starring:
            # 电影导演
            item_loader.add_value('starring', starring)
        else:
            item_loader.add_value('starring', '无')
        # 电影类型
        genre = response.xpath('.//span[@property="v:genre"]/text()').extract()
        if genre:
            item_loader.add_value('genre', genre)
        else:
            item_loader.add_value('genre', '无')
        # 电影时长
        runtime = response.xpath('.//span[@property="v:runtime"]/text()').extract()
        if runtime:
            item_loader.add_value('runtime', runtime)
        else:
            item_loader.add_value('runtime', '0')
        if info:
            # 电影的国别
            item_loader.add_value('country', info[0].strip())
            if len(info) > 1:
                # 电影的语言
                item_loader.add_value('language', info[1].strip())
            else:
                # 电影的语言
                item_loader.add_value('language', '无')
        else:
            item_loader.add_value('country', '无')
            item_loader.add_value('language', '无')
        if rating_num:
            # 电影总评分
            item_loader.add_value('rating_num', rating_num)
        else:
            item_loader.add_value('rating_num', '0')
        if vote_num:
            # 电影评分人数
            item_loader.add_value('vote_num', vote_num)
        else:
            item_loader.add_value('vote_num', '0')

        if rating_per_stars:
            # 电影5分百分比
            item_loader.add_value('rating_per_stars5', rating_per_stars[0].strip())
            # 电影4分百分比
            item_loader.add_value('rating_per_stars4', rating_per_stars[1].strip())
            # 电影3分百分比
            item_loader.add_value('rating_per_stars3', rating_per_stars[2].strip())
            # 电影2分百分比
            item_loader.add_value('rating_per_stars2', rating_per_stars[3].strip())
            # 电影1分百分比
            item_loader.add_value('rating_per_stars1', rating_per_stars[4].strip())
        else:
            # 电影5分百分比
            item_loader.add_value('rating_per_stars5', '0')
            # 电影4分百分比
            item_loader.add_value('rating_per_stars4', '0')
            # 电影3分百分比
            item_loader.add_value('rating_per_stars3', '0')
            # 电影2分百分比
            item_loader.add_value('rating_per_stars2', '0')
            # 电影1分百分比
            item_loader.add_value('rating_per_stars1', '0')
        # 电影短评数
        if related_num:
            item_loader.add_value('comment_num', related_num[0].strip())
        else:
            item_loader.add_value('comment_num', '0')
        # 电影提问数
        if len(related_num) > 1:
            item_loader.add_value('question_num', related_num[1].strip())
        else:
            item_loader.add_value('question_num', '0')
        moive_item = item_loader.load_item()

        yield moive_item
