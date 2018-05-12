#!/usr/bin/env python
# -*- coding: utf-8 -*-
from Database import Database
from HTTPRequest import HTTPRequest

import argparse
import feedparser as fp
import json
import pprint


class FeedParser(object):
    def __init__(self, db, article_parser):
        self._article_parser = article_parser
        self._db = db

    def parse_feed(self, feed_url):
        assert self._db is not None

        # init pretty printer
        pp = pprint.PrettyPrinter(indent=4)

        # get raw feed
        res = HTTPRequest(tries=3, delay=2500).get(feed_url)

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
            res = self._article_parser.parse(entry.link)
            self._db.insert_article(entry.link, feed_url, res.status_code,
                                    res.text)


# add rss feeds from a json file to the database
def add_feeds(args):
    with open(args.add_feeds, "r") as fp:
        feeds = json.loads(fp.read())

    db = Database(args.db_filename)

    for feed in feeds:
        print(f"Updating {feed}")
        db.update_feed(feed["url"], feed["name"], feed["lang"],
                       feed["country"])


# parse command line arguments
def argparse_init():
    parser = argparse.ArgumentParser(description="Feed parser")

    parser.add_argument("-db", "--db-filename", default="articles.db",
                        type=str, help="Database file path")
    parser.add_argument("-add", "--add-feeds", default="(none)", type=str,
                        help="Parse a JSON file and add RSS feeds to the db")
    parser.add_argument("-parser", "--article-parser", default="readability",
                        type=str, help="Article parser (mercury|readability)")
    parser.add_argument("--mercury-api-key", default="(none)", type=str,
                        help="Mercury Web Parser API key")
    parser.add_argument("--readability-log-file", default="(none)", type=str,
                        help="Readability.js server logfile")
    parser.add_argument("--readability-port", default=25287, type=int,
                        help="Readability.js server port")

    args = parser.parse_args()

    if args.add_feeds == "(none)":
        if args.article_parser == "readability":
            if args.readability_log_file == "(none)":
                raise RuntimeError("You need to specify the path to a logfile "
                                   "for the Readability.js server.")
        elif args.article_parser == "mercury":
            if args.mercury_api_key == "(none)":
                raise RuntimeError("You need to specify an API key to use the "
                                   "Mercury Web Parser")
        else:
            raise RuntimeError("Unrecognised article parser")

    return args


# parse all RSS feeds and store the articles in the database
def parse_all_feeds(args):
    db = Database(args.db_filename)

    if args.article_parser == "mercury":
        from MercuryParser import ArticleParser
    elif args.article_parser == "readability":
        from ReadabilityParser import ArticleParser

    article_parser = ArticleParser(args)
    feed_parser = FeedParser(db, article_parser)

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
