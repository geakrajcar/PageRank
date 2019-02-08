# encoding=utf8
import sys

reload(sys)
sys.setdefaultencoding('utf8')

import yaml
import models
import urllib2
from bs4 import BeautifulSoup
from helpers import fullPath
import datetime
import time
from sqlalchemy.sql import or_

conf_f = file('./conf.yaml', 'r')
conf = yaml.load(conf_f)['spider']
conf_f.close()


def dangling_extension(url):
    """"
    Url with dangling extension is marked as COMPLETED wihout fetching
    Dangling extensions are defined in config.yaml
    """
    skip = False
    for ext in conf['skip_exts']:
        if url.endswith(ext):
            return True

    return skip


def debug(string):
    if conf['debug']:
        print string


def start_fetching():
    """
    Starts the fetching proces.
    Requires at least one entry in models.Page.__tablename__
    """
    session = models.Session()

    debug("Started fetching")
    while True:
        page = session.query(models.Page).filter(
            models.Page.parse_status == models.Page.PENDING
        ).first()

        debug("Got %s" % page)
        if page is None:
            debug("Returning...")
            break

        try:
            debug("Fetching... %s" % page.url)
            if dangling_extension(page.url.lower()):
                debug("Dangling extension - skipping")
                page.parse_status = models.Page.COMPLETED
                session.commit()
            else:
                response = urllib2.urlopen(page.url)
        except urllib2.HTTPError as e:
            print "%s: %s" % (e.errno, e.strerror)
            page.parse_status = models.Page.FAILED
            session.commit()
            continue

        page.last_update = datetime.datetime.utcnow()
        page.parse_status = models.Page.COMPLETED
        session.commit()
        debug("Fetch completed.")
        try:
            html = BeautifulSoup(response.read(), "html.parser")
        except Exception as e:
            print e
            continue
        valid_links = {}
        for link in html.find_all('a'):
            if not link.get('href'):
                continue

            href = fullPath(page.url, link.get('href'))

            print "H", href
            if href is None:
                continue

            valid_domain = False
            for domain in conf['domain']:
                if 'http://' + domain in href:
                    valid_domain = True
                if 'https://' + domain in href:
                    valid_domain = True

            print href, valid_domain
            if not valid_domain:
                continue

            if href not in valid_links:
                valid_links[href] = 1
            else:
                valid_links[href] += 1

        debug("Found valid links: %s" % valid_links)
        debug("Processing links")
        for url in valid_links:
            if url.startswith('http'):
                otherUrl = 'https' + url[url.find(':'):]
            else:
                otherUrl = 'http' + url[url.find(':'):]

            destination = session.query(models.Page).filter(or_(
                models.Page.url == url,
                models.Page.url == otherUrl
            )
            ).first()

            debug("Destination: %s" % destination)
            if destination is None:
                destination = models.Page(
                    url=url,
                    parse_status=models.Page.COMPLETED if dangling_extension(url) else models.Page.PENDING
                )
                session.add(destination)
                debug("Destination not found. New one created: %s " % destination)

            link = models.Link(
                from_page=page,
                to_page=destination,
                n=valid_links[url]
            )
            page.outlinks.append(link)
            debug("Created new link: %s" % link)
        session.commit()

if __name__ == '__main__':
    start_fetching()
