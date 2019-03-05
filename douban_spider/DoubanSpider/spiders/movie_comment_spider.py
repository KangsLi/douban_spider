# -*- coding: utf-8 -*-
import re

import scrapy

from urllib import parse as urlparse

from DoubanSpider.items import MovieItemLoader, MovieCommentItem
from utils.DBUtil import MysqlUtil


class MovieCommentSpiderSpider(scrapy.Spider):
    name = 'movie_comment_spider'
    allowed_domains = ['www.douban.com', 'accounts.douban.com', 'movie.douban.com']
    start_urls = ['https://movie.douban.com/']

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
        'Connection': 'keep-alive',
        'Host': 'accounts.douban.com'
    }

    formdata = {
        'name': 'd3041354160@163.com',
        'password': 'dd123456',
        'remember': 'true',
    }

    def start_requests(self):
        return [scrapy.Request(url='https://accounts.douban.com/passport/login',
                               headers=self.headers,
                               meta={'cookiejar': 1},
                               callback=self.login)]

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
            headers=self.headers,
            meta={'cookiejar': response.meta['cookiejar']},
            formdata=self.formdata,
            callback=self.after_login
        )]

    def after_login(self, response):
        self.headers['Host'] = "movie.douban.com"
        sql = "select movie_id from movie_comment group by movie_id having count(*) < 490"
        mysql = MysqlUtil()
        result = mysql.getAll(sql)
        if result:
            for re in result:
                movie_id = re[0]
                for i in range(220, 500, 20):
                    url = "https://movie.douban.com/subject/{0}/comments?start={1}&limit=20&sort=new_score&status=P".format(
                        movie_id, i)
                    yield scrapy.Request(url=url,
                                         headers=self.headers,
                                         meta={'movie_id': movie_id,
                                               'cookiejar': response.meta['cookiejar']},
                                         callback=self.parse_comments
                                         )
        mysql.dispose()


    def parse_comments(self, response):
        comments = response.xpath('//div[@id="comments"]/div[@class="comment-item"]')
        for comment in comments:
            movieItemLoader = MovieItemLoader(item=MovieCommentItem())
            comment_id = comment.xpath('div[@class="comment"]/h3/span[@class="comment-vote"]/input/@value').extract()
            comment_useful_num = comment.xpath(
                'div[@class="comment"]/h3/span[@class="comment-vote"]/span/text()').extract()
            comment_people = comment.xpath('div[@class="avatar"]/a/@title').extract()
            comment_people_url = comment.xpath('div[@class="avatar"]/a/@href').extract()
            comment_movie_star = comment.xpath(
                'div[@class="comment"]/h3/span[@class="comment-info"]/span[2]/@class').extract()[0]
            comment_content = comment.xpath('div[@class="comment"]/p/span[@class="short"]/text()').extract()
            comment_time = comment.xpath(
                'div[@class="comment"]/h3/span[@class="comment-info"]/span[@class="comment-time "]/@title').extract()
            movieItemLoader.add_value("comment_id", comment_id)
            movieItemLoader.add_value("comment_useful_num", comment_useful_num)
            movieItemLoader.add_value("comment_people", comment_people)
            movieItemLoader.add_value("comment_people_url", comment_people_url)
            movieItemLoader.add_value("comment_movie_star", comment_movie_star)
            movieItemLoader.add_value("comment_content", comment_content)
            movieItemLoader.add_value("comment_time", comment_time)
            movieItemLoader.add_value("movie_id", response.meta['movie_id'])
            movieCommentItem = movieItemLoader.load_item()
            yield movieCommentItem
