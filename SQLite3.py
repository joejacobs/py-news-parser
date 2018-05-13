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
    def check_if_article_exists(self, article_url, ignore_protocol=False):
        return self._check_if_item_exists("articles", article_url,
                                          ignore_protocol)

    # check if a feed is already in the database
    def check_if_feed_exists(self, feed_url, ignore_protocol=False):
        return self._check_if_item_exists("feeds", feed_url, ignore_protocol)

    # check if an item exists based on url
    def _check_if_item_exists(self, table, url, ignore_protocol):
        c = self._conn.cursor()

        if ignore_protocol:
            url = "%{}".format(url[url.find("://"):])

        c.execute(f"SELECT * FROM {table} WHERE url LIKE ?", (url,))
        return c.fetchone() is not None

    # check if parsed article is already in the database
    def check_if_parsed_article_exists(self, article_url, parser):
        c = self._conn.cursor()
        c.execute("SELECT * FROM parsed_articles WHERE article=? AND parser=?",
                  (article_url, parser))
        return c.fetchone() is not None

    # check if a website is already in the database
    def check_if_website_exists(self, website_url, ignore_protocol=False):
        return self._check_if_item_exists("websites", website_url,
                                          ignore_protocol)

    # check if tables exist
    def _check_if_tables_exist(self):
        query0 = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        query1 = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        query2 = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        query3 = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"

        c = self._conn.cursor()
        c.execute(query0, ("websites",))

        if c.fetchone() is None:
            return False

        c.execute(query1, ("feeds",))

        if c.fetchone() is None:
            return False

        c.execute(query2, ("articles",))

        if c.fetchone() is None:
            return False

        c.execute(query3, ("parsed_articles",))
        return c.fetchone() is not None

    # create tables if they don't exist
    def _create_tables(self):
        query0 = ("CREATE TABLE IF NOT EXISTS websites ("
                  "url TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, "
                  "language CHAR(2) NOT NULL, country CHAR(2) NOT NULL);")
        query1 = ("CREATE TABLE IF NOT EXISTS feeds ("
                  "url TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, "
                  "website TEXT NOT NULL, "
                  "FOREIGN KEY(website) REFERENCES websites(url));")
        query2 = ("CREATE TABLE IF NOT EXISTS articles ("
                  "url TEXT PRIMARY KEY NOT NULL, "
                  "time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                  "content TEXT NOT NULL, feed TEXT, website TEXT NOT NULL, "
                  "FOREIGN KEY(feed) REFERENCES feeds(url), "
                  "FOREIGN KEY(website) REFERENCES websites(url);")
        query3 = ("CREATE TABLE IF NOT EXISTS parsed_articles ("
                  "article TEXT NOT NULL, parser TEXT NOT NULL, "
                  "content TEXT NOT NULL, PRIMARY KEY(article, parser), "
                  "FOREIGN KEY(article) REFERENCES articles(url));")

        c = self._conn.cursor()
        c.execute(query0)
        c.execute(query1)
        c.execute(query2)
        c.execute(query3)
        self._conn.commit()

    # get a list of feed urls
    def get_all_feed_urls(self):
        c = self._conn.cursor()
        c.execute("SELECT url FROM feeds")
        all_feeds = c.fetchall()
        return [x[0] for x in all_feeds]

    # get a list of all website urls
    def get_all_website_urls(self):
        c = self._conn.cursor()
        c.execute("SELECT url FROM websites")
        all_websites = c.fetchall()
        return [x[0] for x in all_websites]

    # get the root website for a given feed
    def get_website_for_feed(self, feed_url):
        c = self._conn.cursor()
        c.execute("SELECT website FROM feeds WHERE url=?", (feed_url,))
        return c.fetchone()[0]

    # insert an article into the database, return the number of affected rows
    def insert_article(self, article_url, feed_url, website_url, content):
        if not self.check_if_article_exists(article_url, True):
            c = self._conn.cursor()

            if feed_url is None:
                feed_url = ''

            c.execute("INSERT INTO articles (url, feed, website, content) "
                      "VALUES (?, ?, ?, ?)", (article_url, feed_url,
                                              website_url, content))
            assert c.rowcount == 1
            self._conn.commit()
            return 1

        return 0

    # insert an rss feed into the database, return the number of affected rows
    def insert_feed(self, feed_url, website_url, name):
        if not self.check_if_feed_exists(feed_url, True):
            c = self._conn.cursor()
            c.execute("INSERT INTO feeds (url, website, name) "
                      "VALUES (?, ?, ?)", (feed_url, website_url, name))
            assert c.rowcount == 1
            self._conn.commit()
            return 1

        return 0

    # insert parsed article into the database, return the number of affected
    # rows
    def insert_parsed_article(self, article_url, parser, content):
        if not self.check_if_parsed_article_exists(article_url, parser):
            c = self._conn.cursor()
            c.execute("INSERT INTO parsed_articles (article, parser, content) "
                      "VALUES (?, ?, ?)", (article_url, parser, content))
            assert c.rowcount == 1
            self._conn.commit()
            return 1

        return 0

    # insert a website into the database, return the number of affected rows
    def insert_website(self, website_url, name, language, country):
        if not self.check_if_website_exists(website_url, True):
            c = self._conn.cursor()
            c.execute("INSERT INTO websites (url, name, language, country) "
                      "VALUES (?, ?, ?, ?)", (website_url, name, language,
                                              country))
            assert c.rowcount == 1
            self._conn.commit()
            return 1

        return 0

    # insert or update an article
    def update_article(self, article_url, feed_url, website_url, content):
        if feed_url is None:
            feed_url = ''

        if self.insert_article(article_url, feed_url,
                               website_url, content) == 1:
            return 1

        # check if the article protocol changed (e.g. http to https)
        if self.check_if_article_exists(article_url):
            # no changes to protocol so just update article details
            c = self._conn.cursor()
            c.execute("UPDATE articles SET feed=?, website=?, content=? "
                      "WHERE url=?", (feed_url, website_url, content,
                                      article_url))
            assert c.rowcount == 1
            self._conn.commit()
            return 1

        # the article protocol has changed so we need to update the foreign key
        # in the parsed_articles table as well
        c = self._conn.cursor()

        # get old url
        tmp_url = "%{}".format(article_url[article_url.find("://"):])
        c.execute("SELECT url FROM articles WHERE url LIKE ?", (tmp_url,))
        old_url = c.fetchone()[0]

        # update article details
        c.execute("UPDATE articles SET url=?, feed=?, website=?, content=? "
                  "WHERE url=?", (article_url, feed_url, website_url, content,
                                  old_url))
        assert c.rowcount == 1
        self._conn.commit()

        # update foreign key in parsed_articles table
        c.execute("UPDATE parsed_articles SET article=? WHERE article=?",
                  (article_url, old_url))
        rowcount = 1 + c.rowcount
        self._conn.commit()

        return rowcount

    # insert or update an rss feed
    def update_feed(self, feed_url, website_url, name):
        if self.insert_feed(feed_url, website_url, name) == 1:
            return 1

        # check if the feed protocol changed (e.g. http to https)
        if not self.check_if_feed_exists(feed_url):
            c = self._conn.cursor()

            # get old url
            tmp_url = "%{}".format(feed_url[feed_url.find("://"):])
            c.execute("SELECT url FROM feeds WHERE url LIKE ?", (tmp_url,))
            old_url = c.fetchone()[0]

            # update feed details
            c.execute("UPDATE feeds SET url=?, website=?, name=? "
                      "WHERE url=?", (feed_url, website_url, name, old_url))
            assert c.rowcount == 1
            self._conn.commit()

            # update foreign key
            c.execute("UPDATE articles SET feed=? WHERE feed=?", (feed_url,
                                                                  old_url))
            rowcount = 1 + c.rowcount
            self._conn.commit()

            return rowcount

    # insert of update a parsed article, returns the number of affected rows
    def update_parsed_article(self, article_url, parser, content):
        if self.insert_parsed_article(article_url, parser, content) == 1:
            return 1

        c.execute("UPDATE parsed_articles SET content=? WHERE article=? AND "
                  "parser=?", (content, article_url, parser))
        assert c.rowcount == 1
        self._conn.commit()
        return 1

    # insert or update website details, returns the number of affected rows
    def update_website(self, website_url, name, language, country):
        if self.insert_website(website_url, name, language, country) == 1:
            return 1

        # check if the website protocol changed (e.g. http to https)
        if self.check_if_website_exists(url):
            # no changes to protocol so just update website details
            c = self._conn.cursor()
            c.execute("UPDATE websites SET name=?, language=?, country=? "
                      "WHERE url=?", (name, language, country, website_url))
            assert c.rowcount == 1
            self._conn.commit()
            return 1

        # the website protocol has changed so we need to update the foreign key
        # in the articles and feeds tables as well
        c = self._conn.cursor()

        # get old url
        tmp_url = "%{}".format(website_url[website_url.find("://"):])
        c.execute("SELECT url FROM websites WHERE url LIKE ?", (tmp_url,))
        old_url = c.fetchone()[0]

        # update website details
        c.execute("UPDATE websites SET url=?, name=?, language=?, country=? "
                  "WHERE url=?", (website_url, name, language, country,
                                  old_url))
        assert c.rowcount == 1
        self._conn.commit()

        # update foreign key in feeds table
        c.execute("UPDATE feeds SET website=? WHERE website=?", (website_url,
                                                                 old_url))
        rowcount = 1 + c.rowcount
        self._conn.commit()

        # update foreign key in articles table
        c.execute("UPDATE articles SET website=? WHERE website=?",
                  (website_url, old_url))
        rowcount += c.rowcount
        self._conn.commit()

        return rowcount
