# -*- coding: utf-8 -*-
import scrapy

from urllib import parse as urlparse


class DoubanloginSpider(scrapy.Spider):
    name = 'DoubanLogin'
    allowed_domains = ['www.douban.com', 'accounts.douban.com', 'movie.douban.com','music.douban.com']
    start_urls = ['http://www.douban.com/']

    formdata = {
        'name': '3041354160@qq.com',
        'password': 'fzyt9598..',
        'remember': 'true',
    }

    def parse(self, response):
        pass

    def start_requests(self):
        return [scrapy.Request(url='https://accounts.douban.com/passport/login',
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
            formdata=self.formdata,
            callback=self.after_login
        )]

    def after_login(self, response):
        return [scrapy.Request(
            url='https://movie.douban.com/explore',
            callback=self.parse_movie
        )]

    def parse_movie(self, response):
        url = "https://www.douban.com"
        yield scrapy.Request(
            url,
            callback=self.parse_json
        )

    def parse_json(self, response):
        yield scrapy.Request(
            url='https://music.douban.com/',
            callback=self.parse
        )
