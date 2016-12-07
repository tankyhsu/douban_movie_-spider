# coding=utf8
import traceback

import requests
import codecs
import MySQLdb
import time
import sys

from bs4 import BeautifulSoup

DOWNLOAD_URL = u'https://movie.douban.com/tag/%E7%BE%8E%E5%9B%BD%20%E6%81%90%E6%80%96?type=T'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/47.0.2526.80 Safari/537.36 '
}

DB = "douban.db"

CREATE_SQL = """DROP TABLE IF EXISTS `movie`;
                  CREATE TABLE `movie` (
                  `id` int(11) NOT NULL AUTO_INCREMENT,
                  `name` varchar(255) DEFAULT '',
                  `description` longtext,
                  `year` int(11) DEFAULT NULL,
                  `catalog` varchar(50) DEFAULT '',
                  `region` varchar(50) DEFAULT '',
                  PRIMARY KEY (`id`)
                 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""


def download_page(url):
    data = requests.get(url, headers=HEADERS).content
    return data


def parse_movie_detail(movie_url):
    detail_html = download_page(movie_url)
    soup = BeautifulSoup(detail_html)

    movie_title = soup.find('span', attrs={'property': 'v:itemreviewed'}).string
    description = soup.find('span', attrs={'property': 'v:summary'}).contents
    movie_description = ""
    for line in description:
        if str(line) == '<br/>':
            continue
        else:
            movie_description += line.strip().rstrip()
    movie_description.strip().rstrip()
    movie_region = u"美国"
    movie_catalog = u"恐怖"
    movie_year = soup.find('span', attrs={'class': 'year'}).string
    movie_year = int(movie_year[1:5])

    movie = (movie_title, movie_description, movie_year, movie_catalog, movie_region)
    return movie


def parse_html(html):
    soup = BeautifulSoup(html)
    movie_list_soup = soup.find('div', attrs={'class': 'article'}).find('div', attrs={'class': ''})
    movie_name_list = []

    conn, cursor = create_connenction()

    try:
        for movie_table in movie_list_soup.find_all('table'):
            detail = movie_table.find('tr', attrs={'class': 'item'})
            movie_name = detail.find('a', attrs={'class': 'nbg'})["title"]
            movie_url = detail.find('a', attrs={'class': 'nbg'})['href']
            movie = parse_movie_detail(movie_url)
            sql = "INSERT IGNORE INTO movie VALUES(NULL, '%s', '%s', '%d', '%s', '%s')" % movie
            print sql
            cursor.execute(sql)
            movie_name_list.append(movie_name)
            time.sleep(1)

    except Exception:
        print Exception
        # 打印错误信息
        traceback.print_exc()

    finally:
        # 关闭Cursor:
        cursor.close()
        # 提交事务:
        conn.commit()
        # 关闭Connection:
        conn.close()

    # find the next page
    next_page = soup.find('span', attrs={'class': 'next'}).find('a')

    # catenate the page url
    if next_page:
        return movie_name_list, next_page['href']

    return movie_name_list, None


def create_connenction():
    # 连接mysql数据库，user为数据库的名字，passwd为数据库的密码，一般把要把字符集定义为utf8
    conn = MySQLdb.connect(host='localhost', user='root', passwd='1qaz2wsx?', charset='utf8')
    cursor = conn.cursor()  # 获取操作游标
    cursor.execute('use spider')  # 使用douban这个数据库
    return conn, cursor


def main():
    reload(sys)
    sys.setdefaultencoding('utf8')

    url = DOWNLOAD_URL

    # create table
    conn, cursor = create_connenction()
    cursor.execute(CREATE_SQL)
    cursor.close()
    conn.commit()
    conn.close()

    with codecs.open('_movies.text', 'wb', encoding='utf-8') as fp:
        while url:
            # get the page
            html = download_page(url)
            # analysize the page
            movies, url = parse_html(html)
            fp.write(u'{movies}\n'.format(movies='\n'.join(movies)))
            # fp.write(u'{url}\n'.format(url='\n'.join(url)))


if __name__ == '__main__':
    main()
