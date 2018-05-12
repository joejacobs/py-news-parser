#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sqlite3


class Database(object):
    # init the db connection
    def __init__(self, db_filename):
        self._conn = sqlite3.connect(db_filename)

        if not self._check_if_tables_exist():
            self._create_tables()

    # close the db
    def __del__(self):
        self._conn.commit()
        self._conn.close()

    # check if an article is already in the database
    def check_if_article_exists(self, url):
        c = self._conn.cursor()
        c.execute("SELECT * FROM articles WHERE url=?", (url,))
        return c.fetchone() is not None

    # check if a feed is already in the database
    def _check_if_feed_exists(self, url, ignore_protocol=False):
        c = self._conn.cursor()

        if ignore_protocol:
            url = "%{}".format(url[url.find("://"):])

        c.execute("SELECT * FROM feeds WHERE url LIKE ?", (url,))
        return c.fetchone() is not None

    # check if tables exist
    def _check_if_tables_exist(self):
        query1 = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        query2 = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"

        c = self._conn.cursor()
        c.execute(query1, ("feeds",))

        if c.fetchone() is None:
            return False

        c.execute(query2, ("articles",))
        return c.fetchone() is not None

    # create tables if they don't exist
    def _create_tables(self):
        query1 = ("CREATE TABLE IF NOT EXISTS feeds ("
                  "url TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, "
                  "language CHAR(2) NOT NULL, country CHAR(2) NOT NULL);")
        query2 = ("CREATE TABLE IF NOT EXISTS articles ("
                  "url TEXT PRIMARY KEY NOT NULL, feed_url TEXT NOT NULL, "
                  "time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                  "response_status INT NOT NULL, "
                  "response_content TEXT NOT NULL, "
                  "FOREIGN KEY(feed_url) REFERENCES feeds(url));")

        c = self._conn.cursor()
        c.execute(query1)
        c.execute(query2)
        self._conn.commit()

    # get a list of feed urls
    def get_all_feed_urls(self):
        c = self._conn.cursor()
        c.execute("SELECT url FROM feeds")
        all_feed_urls = c.fetchall()
        return [x[0] for x in all_feed_urls]

    # get a list of all website urls
    def get_all_website_urls(self):
        c = self._conn.cursor()
        c.execute("SELECT url FROM websites")
        all_website_urls = c.fetchall()
        return [x[0] for x in all_website_urls]

    # insert an article into the database
    def insert_article(self, url, feed_url, response_status, response_content):
        c = self._conn.cursor()
        c.execute("INSERT INTO articles (url, feed_url, response_status, "
                  "response_content) VALUES (?, ?, ?, ?)", (url, feed_url,
                                                            response_status,
                                                            response_content))
        assert c.rowcount == 1
        self._conn.commit()

    # insert a new rss feed into the database, returns the number of affected
    # rows
    def insert_feed(self, url, name, language, country):
        if not self._check_if_feed_exists(url, True):
            c = self._conn.cursor()
            c.execute("INSERT INTO feeds (url, name, language, country)"
                      "VALUES (?, ?, ?, ?)", (url, name, language, country))
            assert c.rowcount == 1
            self._conn.commit()
            return 1

        return 0

    # insert or update rss feed details, returns the number of affected rows
    def update_feed(self, url, name, language, country):
        if self.insert_feed(url, name, language, country) == 1:
            return 1

        # check if the feed protocol changed (e.g. http to https)
        if self._check_if_feed_exists(url):
            # no changes to protocol so just update feed details
            c = self._conn.cursor()
            c.execute("UPDATE feeds SET name=?, language=?, country=? "
                      "WHERE url=?", (name, language, country, url))
            rowcount = c.rowcount
            self._conn.commit()
            return rowcount

        # the feed protocol has changed so we need to update the foreign key in
        # the articles table as well
        c = self._conn.cursor()

        # get old url
        tmp_url = "%{}".format(url[url.find("://"):])
        c.execute("SELECT url FROM feeds WHERE url LIKE ?", (tmp_url,))
        old_url = c.fetchone()[0]

        # update feed details
        c.execute("UPDATE feeds SET url=?, name=?, language=?, country=? "
                  "WHERE url=?", (url, name, language, country, old_url))
        assert c.rowcount == 1
        self._conn.commit()

        # update foreign key
        c.execute("UPDATE articles SET feed_url=? WHERE feed_url=?",
                  (url, old_url))
        rowcount = 1 + c.rowcount
        self._conn.commit()

        return rowcount
