#!/usr/bin/env python3
# -*- coding: utf8 -*-
import argparse
import urllib.request
import re
import psycopg2
from urllib.parse import urlparse
from time import sleep
import logging
import sys
from itertools import groupby
from urllib.error import HTTPError, URLError
from multiprocessing.dummy import Pool as ThreadPool
from datetime import datetime
from config import DB


EXCLUDE = ['.jpg', '.png', '.pdf', '.psd', '.gif', '.avi', '.mpeg', '.mov',
             '.flac', '.flv', '.mkv', '.dvd', '.odt', '.xls', '.doc', '.docx',
              '.xlsx', '.mpp', '.zip', '.tar', '.rar', '.tumblr', '.xml']

# TODO:
# 1. Add argument parser
# 2. Refactoring logging
# 3  Personal log file for every proccess
# 4. README
# 5. Release on github and bitbucket
# 6. Refactoring
# 7. Syslog server
# 8. Rework cookie parser
# 9. Fix id numbers
# 10. Check for internal links like "support.html"
# 11. if external links too much then part of them add to DB
# 12. if // then this is external link
# 13. Docker container
# 14. Export urls in few formats (xml, raw, cvs, sql, ...)
# 15. Fix RE on external links

def getInternalLinks(includeUrl, origUrl, procNumb, bankIncludeUrl=[], deep=0, deepRecurs=0):
    try:
        deepRecurs += 1
        internalLinks = []
        html, cookie = requestSite(includeUrl, procNumb)
        tempurl = urlparse(includeUrl).netloc
        rr = re.compile('href="(\/[^"]*|\?[^"]*|[^"]*'+tempurl+'[^"]*)"')
        bankIncludeUrl = unicList(bankIncludeUrl)
        tempLinks = []
        for link in re.findall(rr, html.decode('utf-8')):
            if link is not None:
                if tempurl not in link:
                    linkFull = origUrl+link
                else:
                    linkFull = link
                if linkFull not in bankIncludeUrl:
                    if link != '/':
                            tempLinks.append(linkFull)
                            bankIncludeUrl.append(linkFull)
        if tempLinks:
            for l in tempLinks:
                if deepRecurs <= deep:
                    bankIncludeUrl += getInternalLinks(l, origUrl, procNumb, bankIncludeUrl=bankIncludeUrl, deepRecurs=deepRecurs)
        internalLinks += bankIncludeUrl
        internalLinks = unicList(internalLinks)
        return internalLinks
    except:
        logging.debug('Process #%s = ERROR in internal link: %s', procNumb, includeUrl)
        return ''


def unicList(listItem):
    listItem.sort()
    listItem = [el for el, _ in groupby(listItem)]
    return listItem

def getExternalLinks(url, procNumb):
    try:
        html, cookie = requestSite(url, procNumb)
        logging.debug('Process #%s = START parcing external link: %s', procNumb, url)
        rr= re.compile('href="((http|www)[^"]+)"')
        externalLinks = []
        for link in re.findall(rr, html.decode('utf-8')):
            if link[0] is not None:
                if url not in link[0]:
                    if link[0] not in externalLinks:
                        if '.tumblr' not in link[0]:
                            p = urlparse(link[0])
                            if p.netloc and '.' in p.netloc:
                                externalLinks.append(p.scheme +'://'+ p.netloc)
        logging.debug('Process #%s = STOP parcing external link: %s', procNumb, url)
        return externalLinks
    except:
        logging.debug('Process #%s = ERROR in external link: %s', procNumb, url)
        return ''

def requestSite(url, procNumb):
    sleep(0.25)
    for ex in EXCLUDE:
        if ex in url:
            return '', ''
    try:
        req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:62.0) Gecko/20100101 Firefox/62.0'})
        logging.debug('Process #%s = Trying opening url: %s', procNumb, url)
        r = urllib.request.urlopen(req, timeout=3)
        logging.debug('Process #%s = url opened: %s', procNumb, url)
        html = r.read()
        cookie = r.getheader('Set-Cookie')
        if html:
            return html, cookie
        else:
            return '', ''
    except HTTPError as e:
        logging.debug('Process #%s = ERROR when opening url(%s) with error %s', procNumb, url, e)
        return '', ''
    except URLError as e:
        logging.debug('Process #%s = ERROR when opening url(%s) with error %s', procNumb, url, e)
        return '', ''
    except:
        logging.debug('Process #%s = ERROR when opening url(%s) with unknown error', procNumb, url)
        return '', ''

def crawling(potok):
    procNumb = str(potok)
    while True:
        try:
            logging.info('Process #%s = +++++++NEW ROUND+++++++', procNumb)
            logging.info('Process #%s = Connecting to DB: %s', procNumb, args.address)
            conn = psycopg2.connect(host=args.address, user=DB['username'], password=DB['password'], dbname=DB['dbname'])
            logging.debug('Process #%s = Initializating cursor ', procNumb)
            c = conn.cursor()
            logging.debug('Process #%s = Execute SELECT from DB', procNumb)
            c.execute('SELECT url FROM urls WHERE checked=0 ORDER BY random() LIMIT 1 FOR UPDATE;')
            db_urls = c.fetchall()
            logging.debug('Process #%s = Fetched from DB', procNumb)
            if db_urls:
                url = db_urls[0][0]
                c.execute('UPDATE urls SET checked=1 WHERE url=%s', (url,))
                html, cookie = requestSite(url, procNumb)
                if cookie:
                    c.execute('INSERT INTO cookies(url, cookie) VALUES (%s, %s)', (url, cookie))
            conn.commit()
            conn.close()
            logging.info('Process #%s = Crawling site: %s', procNumb, url)
            externalLinks = []
            logging.debug('Process #%s = requsting url: %s', procNumb, url)
            internalLinks = getInternalLinks(url, url, procNumb)
            if not internalLinks:
                logging.debug('Process #%s = internalLinks is empty in URL: %s', procNumb, url)
                logging.info('Process #%s = Crawling url %s DONE', procNumb, url)

            else:
                internalLinks.append(url)
                logging.debug('Process #%s = internalLinks done in URL: %s', procNumb, url)
                internalLinks = unicList(internalLinks)
                for inter in internalLinks:
                    externalLinks += getExternalLinks(inter, procNumb)
                    if not externalLinks:
                        continue
                    logging.debug('Process #%s = externalLinks Done in URL: %s', procNumb, inter)
                externalLinks = unicList(externalLinks)
                logging.info('Process #%s = Connecting to DB for external: %s', procNumb, args.address)
                conn_ext = psycopg2.connect(host=args.address, user=DB['username'], password=DB['password'], dbname='urls')
                logging.debug('Process #%s = Initializating cursor for ext', procNumb)
                c_ext = conn_ext.cursor()
                if externalLinks:
                    extNums = 0
                    for cl_ex in externalLinks:
                        c_ext.execute('SELECT url FROM urls where url=%s', (cl_ex,))
                        in_db = c_ext.fetchone()
                        if not in_db:
                            try:
                                logging.debug('Process #%s = url %s not in DB, adding...', procNumb, cl_ex)
                                c_ext.execute('INSERT INTO urls(url, url_source, datetime, checked) VALUES (%s, %s, %s, 0)', (cl_ex, url, datetime.now(),))
                                extNums += 1
                                conn_ext.commit()
                            except psycopg2.OperationalError as e:
                                logging.debug('Process #%s = ERROR url %s not added, %s', procNumb, cl_ex, e)
                    conn_ext.close()
                    if extNums:
                        logging.info('Process #%s = In url %s added %s external links', procNumb, url, str(extNums))
                        logging.info('Process #%s = Crawling url %s DONE', procNumb, url)
                    else:
                        logging.info('Process #%s = In url %s all external links exists', procNumb, url)
                        logging.info('Process #%s = Crawling url %s DONE', procNumb, url)
                else:
                    conn_ext.close()
                    logging.info('Process #%s = ERROR external links dont found: %s', procNumb, url)
                    logging.info('Process #%s = Crawling url %s DONE', procNumb, url)
        except:
                logging.debug('ERROR - Process #%s IS SHUTTING DOWN', procNumb)


def main(threads=5):
    logging.info('------START PROGRAM------')
    procs = []
    for i in range(1, threads+1):
        procs.append(i)
    pool = ThreadPool(threads)
    pool.map(crawling, procs)
    pool.close()
    pool.join()


"""
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Enter ip, level of logging and number of threads')
        sys.exit()
    else:
        if sys.argv[2] == 'DEBUG':
            level_debug = 10
        elif sys.argv[2] == 'INFO':
            level_debug = 20
        FORMAT = '%(asctime)-15s %(message)s'
        logging.basicConfig(filename='work.log', level=level_debug, format=FORMAT)
        logging.info('Start program')
        if len(sys.argv) == 4:
            logging.debug('Parcing arguments: %s %s %s', args.address, sys.argv[2], sys.argv[3])
            main(int(sys.argv[3]))
        else:
            main()
"""

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-a','--address',type=str,help='default ip address is 127.0.0.1',default='127.0.0.1',metavar='')
    parser.add_argument('-v','--verbose',action='count',help='Level of logging (INFO,DEBUG)',default=1)
    parser.add_argument('-t','--threads',type=int,help='Number of threads',default=1,metavar='')
    args=parser.parse_args()

    if args.verbose == 1:
        level_debug = 10
    elif args.verbose == 2:
        level_debug = 20


    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(filename='work.log', level=level_debug, format=FORMAT)
    logging.info('Start program')


    logging.debug('Parcing arguments: %s %s %s', args.address, args.verbose, args.threads)
    main(args.threads)

