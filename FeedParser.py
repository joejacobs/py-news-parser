#!/usr/bin/env python
# -*- coding: utf-8 -*-
from HTTPRequest import HTTPRequest
from SQLite3 import Database

import argparse
import datetime
import feedparser as fp
import json


class FeedParser(object):
    def __init__(self, db, user_agent=None):
        self._db = db
        self._user_agent = user_agent

    def parse_feed(self, feed_url):
        assert self._db is not None

        # get raw feed
        res = HTTPRequest(2500, 3, self._user_agent).get(feed_url)

        if res.status_code != 200:
            print(f"\t{res.status_code}")
            print(f"\t{res.text}".replace("\n", "\n\t"))
            return

        # parse feed
        feed = fp.parse(res.text)

        for entry in feed.entries:
            # check if the feed item is already in the database
            if self._db.check_if_article_exists(entry.link):
                continue

            # pass the article to the article parser and store any response
            print(f"\t{entry.link}")
            website_url = self._db.get_website_for_feed(feed_url)
            self._db.insert_article(entry.link, feed_url, website_url,
                                    json.dumps(entry))


# add rss feeds from a json file to the database
def add_feeds(args):
    with open(args.add_feeds, "r") as f:
        feeds = json.loads(f.read())

    db = Database(args.db_filename)

    for feed in feeds:
        print(f"Updating {feed}")

        feed_url = feed["url"]
        feed_name = feed["name"]
        website_url = feed["website_url"]
        website_name = feed["website_name"]
        website_lang = feed["website_language"]
        website_country = feed["website_country"]

        db.update_website(website_url, website_name, website_lang,
                          website_country)
        db.update_feed(feed_url, website_url, feed_name)


# parse command line arguments
def argparse_init():
    parser = argparse.ArgumentParser(description="Feed parser")

    parser.add_argument("-db", "--db-filename", default="articles.db",
                        type=str, help="Database file path")
    parser.add_argument("-ua", "--user-agent", default="(none)", type=str,
                        help="A custom user agent to use for HTTP requests")
    parser.add_argument("-add", "--add-feeds", default="(none)", type=str,
                        help="Parse a JSON file and add RSS feeds to the db")

    args = parser.parse_args()

    if args.user_agent == "(none)":
        args.user_agent = None

    return args


# parse all RSS feeds and store the articles in the database
def parse_all_feeds(args):
    # append year and month to db_filename
    now = datetime.datetime.utcnow()
    year = now.year()
    month = now.month()

    db_filename = args.db_filename.split(".")
    db_filename[-1] = "{}.{}.{}".format(year, month, db_filename[-1])
    db_filename = ".".join(db_filename)

    db = Database(db_filename)
    feed_parser = FeedParser(db, args.user_agent)

    for url in db.get_all_feed_urls():
        print(f"Parsing {url}")
        feed_parser.parse_feed(url)


def main():
    args = argparse_init()

    if args.add_feeds == "(none)":
        # parse feeds
        parse_all_feeds(args)
    else:
        # add/update feeds
        add_feeds(args)


if __name__ == "__main__":
    main()
