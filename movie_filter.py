# coding=utf8
import traceback

import requests
import codecs

import sqlite3
from bs4 import BeautifulSoup

DOWNLOAD_URL = u'https://movie.douban.com/tag/%E7%BE%8E%E5%9B%BD%20%E6%81%90%E6%80%96?type=T'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36'
}

DB = "douban.db"

CREATE_SQL = 'CREATE TABLE movie(' \
             'id INTEGER PRIMARY KEY,' \
             'name TEXT,' \
             'description TEXT,' \
             'year INTEGER,' \
             'catalog TEXT,' \
             'region TEXT);'
CREATE_INDEX_SQL = 'CREATE UNIQUE INDEX autoindex_movie_1 ON movie (name);'


def download_page(url):
    data = requests.get(url, headers=HEADERS).content
    return data


def parse_movie_detail(movie_url):
    detail_html = download_page(movie_url)
    soup = BeautifulSoup(detail_html)

    movie_title = soup.find('span', attrs={'property': 'v:itemreviewed'}).string
    movie_description = str(soup.find('div', attrs={'id': 'link-report'}))
    movie_region = u"美国"
    movie_catalog = u"恐怖"
    movie_year = soup.find('span', attrs={'class': 'year'}).string
    movie_year = int(movie_year[1:5])

    movie = [movie_title, movie_description, movie_year, movie_catalog, movie_region]
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
            cursor.execute("INSERT OR IGNORE INTO movie VALUES \
                            (NULL, ?, ?, ?, ?, ?)", movie)
            movie_name_list.append(movie_name)


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
    # 连接到SQLite数据库
    # 数据库文件是test.db
    # 如果文件不存在，会自动在当前目录创建:
    conn = sqlite3.connect(DB)
    conn.text_factory = str
    # 创建一个Cursor:
    cursor = conn.cursor()
    return conn, cursor


def main():
    url = DOWNLOAD_URL

    # create table
    conn, cursor = create_connenction()
    cursor.execute(CREATE_SQL)
    cursor.execute(CREATE_INDEX_SQL)
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
