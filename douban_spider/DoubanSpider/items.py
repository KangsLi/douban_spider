# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import re

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst, Join


class DoubanspiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


def get_date(value):
    match_re = re.match(".*?(\d+-\d+-\d+).*", value)
    if match_re:
        return match_re.group(1)
    else:
        return '2000-01-01'


def get_num(value):
    match_re = re.match(".*?([0-9]+).*", value)
    if match_re:
        return match_re.group(1)
    else:
        return "-1"


def return_value(value):
    return value


def to_float(value):
    if value:
        value = float(value.strip("%")) / 100
    else:
        value = 0.0
    return value


def is_null(value):
    if value:
        return value
    else:
        return "无"


class MovieItemLoader(ItemLoader):
    # 自定义itemloader
    default_output_processor = TakeFirst()


# movie_item
class DoubanMovieItem(scrapy.Item):
    movie_id = scrapy.Field(  # 电影的唯一ID
        input_processor=MapCompose(get_num)
    )
    url = scrapy.Field()  # 电影的url
    movie_title = scrapy.Field()  # 电影名字
    release_date = scrapy.Field(  # 电影上映日期
        input_processor=MapCompose(get_date)
    )
    directedBy = scrapy.Field(  # 电影导演
        input_processor=MapCompose(return_value),
        output_processor=Join(',')
    )
    screenwriter = scrapy.Field(  # 电影编剧
        input_processor=MapCompose(return_value),
        output_processor=Join(',')
    )
    starring = scrapy.Field(  # 电影主演
        input_processor=MapCompose(return_value),
        output_processor=Join(',')
    )
    genre = scrapy.Field(  # 电影类型
        input_processor=MapCompose(return_value),
        output_processor=Join(',')
    )
    runtime = scrapy.Field(  # 电影时长
        input_processor=MapCompose(get_num)
    )
    country = scrapy.Field()  # 电影的国别
    language = scrapy.Field()  # 电影的语言
    rating_num = scrapy.Field()  # 电影总评分
    vote_num = scrapy.Field()  # 电影评分人数
    rating_per_stars5 = scrapy.Field(  # 电影5分百分比
        input_processor=MapCompose(to_float)
    )
    rating_per_stars4 = scrapy.Field(  # 电影4分百分比
        input_processor=MapCompose(to_float)
    )
    rating_per_stars3 = scrapy.Field(  # 电影3分百分比
        input_processor=MapCompose(to_float)
    )
    rating_per_stars2 = scrapy.Field(  # 电影2分百分比
        input_processor=MapCompose(to_float)
    )
    rating_per_stars1 = scrapy.Field(  # 电影1分百分比
        input_processor=MapCompose(to_float)
    )
    comment_num = scrapy.Field(  # 电影短评数
        input_processor=MapCompose(get_num)
    )
    question_num = scrapy.Field(  # 电影提问数
        input_processor=MapCompose(get_num)
    )

    def get_insert_sql(self):
        insert_sql = """
            insert into movie_item_top_250(movie_id,url,movie_title,release_date,directedBy,screenwriter,
                                    starring,genre,runtime,country,language,rating_num,vote_num,rating_per_stars5,
                                    rating_per_stars4,rating_per_stars3,rating_per_stars2,rating_per_stars1,
                                    comment_num,question_num)
                   values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        params = (self['movie_id'], self['url'], self['movie_title'], self['release_date'], self['directedBy'],
                  self['screenwriter'],
                  self['starring'], self['genre'], self['runtime'], self['country'], self['language'],
                  self['rating_num'],
                  self['vote_num'], self['rating_per_stars5'], self['rating_per_stars4'], self['rating_per_stars3'],
                  self['rating_per_stars2'], self['rating_per_stars1'], self['comment_num'], self['question_num'])
        return insert_sql, params

    def get_update_sql(self):
        update_sql = """
            update movie_item set movie_title = %s, release_date = %s, directedBy = %s,
                                   screenwriter = %s, starring = %s, genre = %s, runtime = %s,
                                   country = %s, language = %s, rating_num = %s, vote_num = %s,
                                   rating_per_stars5 = %s, rating_per_stars4 = %s, rating_per_stars3 = %s,
                                   rating_per_stars2 = %s, rating_per_stars1 = %s, comment_num = %s,
                                   question_num = %s where movie_id = %s
        """
        params = (self['movie_title'], self['release_date'], self['directedBy'],
                  self['screenwriter'],
                  self['starring'], self['genre'], self['runtime'], self['country'], self['language'],
                  self['rating_num'],
                  self['vote_num'], self['rating_per_stars5'], self['rating_per_stars4'], self['rating_per_stars3'],
                  self['rating_per_stars2'], self['rating_per_stars1'], self['comment_num'], self['question_num'],
                  self['movie_id'])
        return update_sql, params


def to_utf8(value):
    return value.encode('utf-8')

class MovieCommentItem(scrapy.Item):
    comment_id = scrapy.Field()
    comment_useful_num = scrapy.Field(
        input_processor=MapCompose(get_num)
    )
    comment_people = scrapy.Field(
        output_processor = MapCompose(to_utf8)
    )
    comment_people_url = scrapy.Field()
    comment_movie_star = scrapy.Field(
        input_processor=MapCompose(get_num)
    )
    comment_content = scrapy.Field(
        output_processor=MapCompose(to_utf8)
    )
    comment_time = scrapy.Field(
        input_processor=MapCompose(get_date)
    )
    movie_id = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            insert into movie_comment(comment_id,comment_useful_num,comment_people,comment_people_url,comment_movie_star,comment_content,comment_time,movie_id)
                   values(%s,%s,%s,%s,%s,%s,%s,%s)
        """

        params = (self['comment_id'], self['comment_useful_num'], self['comment_people'],
                  self['comment_people_url'], self['comment_movie_star'],
                  self['comment_content'],
                  self['comment_time'], self['movie_id'])
        return insert_sql, params
