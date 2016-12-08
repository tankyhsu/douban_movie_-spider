# coding=utf-8

import sqlite3


class MovieDatabase(object):
    def __init__(self, db):
        self.db = db

    def create_connection(self):
        # 连接到SQLite数据库
        # 数据库文件是test.db
        # 如果文件不存在，会自动在当前目录创建:
        self.conn = sqlite3.connect(self.db)
        # 创建一个Cursor:
        self.cursor = self.conn.cursor()

    def close_connenction(self):
        # 关闭Cursor:
        self.cursor.close()
        # 提交事务:
        self.conn.commit()
        # 关闭Connection:
        self.conn.close()

    def query(self, sql):
        self.create_connection()
        # 执行查询语句:
        self.cursor.execute(sql)
        # 获得查询结果集:
        values = self.cursor.fetchall()
        self.close_connenction()
        return values

