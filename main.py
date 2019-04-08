#!/usr/bin/env python3
# -*- coding: utf8 -*-

import argparse
import logging
import re
import urllib.request
from datetime import datetime
from itertools import groupby
from multiprocessing.dummy import Pool as ThreadPool
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

import psycopg2

from config import DB

# Extensions that need to be circumvented so that the pages load quickly enough
EXCLUDE = ['.jpg', '.png', '.pdf', '.psd', '.gif', '.avi', '.mpeg', '.mov',
             '.flac', '.flv', '.mkv', '.dvd', '.odt', '.xls', '.doc', '.docx',
              '.xlsx', '.mpp', '.zip', '.tar', '.rar', '.tumblr', '.xml']


# TODO:
# 1. README
# 2. Release on github
# 3. Check for internal links like "support.html"
# 4. Export urls in few formats (sql)
# 5. Add program for create linked graph



def getInternalLinks(includeUrl, origUrl, procNumb, bankIncludeUrl=[], deep=0, deepRecurs=0):
    # Recursive function, which is necessary to search for links that are inside the opened links.
    # Input: 
        # includeUrl - str - site's internal link 
        # origUrl - str - original URL of site
        # procNumb - int - thread number
        # bankIncludeUrl - list - is an array of internal links found on site, 
        # deep - int - is the depth level of the allowed research
        # deepRecurs - the level of currently deepening.
    # Return - list - a list of found links within the site

    try:
        deepRecurs += 1
        internalLinks = []
        html, _ = requestSite(includeUrl, procNumb)
        tempurl = urlparse(includeUrl).netloc
        # Compile regexp for search internal links
        rr = re.compile('href="(\/[^"]*|\?[^"]*|[^"]*' + tempurl + '[^"]*)"')
        bankIncludeUrl = unicList(bankIncludeUrl)
        tempLinks = []
        # Parsing url links by regular expression
        for link in re.findall(rr, html.decode('utf-8')):
            if link:
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
                # Deepening search of site according to the deep level
                if deepRecurs <= deep:
                    bankIncludeUrl += getInternalLinks(l, origUrl, procNumb, bankIncludeUrl=bankIncludeUrl, deepRecurs=deepRecurs)
        internalLinks += bankIncludeUrl
        internalLinks = unicList(internalLinks)
        return internalLinks
    except:
        logging.debug('Process #%s = ERROR in internal link: %s', procNumb, includeUrl)
        return ''


def unicList(listItem):
    # The function excluding repetitions in the list of links.
    # Input:
        # listItem - list of string - the list of links that need to be filtered
    # Return - list - a list of links with unique url

    listItem.sort()
    listItem = [el for el, _ in groupby(listItem)]
    return listItem


def getExternalLinks(url, procNumb):
    # Function to get external links on the resulting site url
    # Input:
        # url - str - internal link found while parsing the resulting site
        # prucNumb - int - thread number
    # Return - list - the found external references if successful, or null if unsuccessful.

    try:
        html, _ = requestSite(url, procNumb)
        urlHost = urlparse(url)
        logging.debug('Process #%s = START parcing external link: %s', procNumb, url)
        rr= re.compile('href="((http|www|\/\/)[^"]+)"')
        externalLinks = []
        for link in re.findall(rr, html.decode('utf-8')):
            if link[0]:
                if urlHost.netloc not in link[0]:
                    p = urlparse(link[0])
                    if p.netloc and '.' in p.netloc:
                        if p.scheme:
                            extLink = p.scheme + '://' + p.netloc
                        else:
                            extLink = urlHost.scheme + '://' + p.netloc
                        if extLink not in externalLinks:
                            externalLinks.append(extLink)
                    
        logging.debug('Process #%s = STOP parcing external link: %s', procNumb, url)
        return externalLinks
    except:
        logging.debug('Process #%s = ERROR in external link: %s', procNumb, url)
        return ''


def requestSite(url, procNumb):
    # The function requests the next site for research.
    # Input:
        # url - str - the intended address of the site for further research
        # procNumb - int - thread number
    # Return:
        # html - str - html code of requsted site
        # cookie - str - set-cookie header of site response.

    sleep(0.25)
    # Checking for the presence in the files of the extension contained in EXCLUDE
    for ex in EXCLUDE:
        if ex in url:
            return '', ''

    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:62.0) Gecko/20100101 Firefox/62.0'})
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
    # The main investigative function
    # Input:
        # potok - int - thread number

    procNumb = str(potok)
    while True:
        try:
            logging.info('Process #%s = +++++++NEW ROUND+++++++', procNumb)
            logging.info('Process #%s = Connecting to DB: %s', procNumb, args.address)

            # Database connection
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
                _, cookie = requestSite(url, procNumb)   # Request for found link
                if cookie:
                    c.execute('INSERT INTO cookies(url, cookie) VALUES (%s, %s)', (url, cookie))
                    logging.info('Process #%s = Saved cookie from %s', procNumb, url)
                    logging.debug('Process #%s = Saved cookie %s from %s', procNumb, cookie, url)

            conn.commit()
            conn.close()

            logging.info('Process #%s = Crawling site: %s', procNumb, url)
            externalLinks = []
            logging.debug('Process #%s = requsting url: %s', procNumb, url)

            internalLinks = getInternalLinks(url, url, procNumb,deep=args.deep)

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


def main(threads=1):
    # Performing function
    # Input:
        # threads -int - takes as many streams as input

    logging.info('------START PROGRAM------')
    procs = []
    for i in range(1, threads+1):
        procs.append(i)
    pool = ThreadPool(threads)
    pool.map(crawling, procs)
    pool.close()
    pool.join()


if __name__ == '__main__':
    # The main body of the program.
    # Input: 
        # a - string - ip address of the device on which the database is located, by default 127.0.0.1
        # v - count of 'v' - the logging level
        # t - int - the number of threads.

    parser = argparse.ArgumentParser(description='Example: python main.py -a 127.0.0.1 -v -t 5 -d 0')
    parser.add_argument('-a', '--address', type=str, help='default ip address is 127.0.0.1',
                        default='127.0.0.1', metavar='')
    parser.add_argument('-v', '--verbose', help='Level of logging (INFO,DEBUG)',
                        action='count', default=0)
    parser.add_argument('-t', '--threads', type=int, help='Number of threads',
                        default=1, metavar='')
    parser.add_argument('-d', '--deep', type=int, help='Deep of recursion',
                        default=0,metavar='')
    args=parser.parse_args()

    if args.verbose == 1:
        level_debug = 20
    elif args.verbose == 2:
        level_debug = 10
    else:
        level_debug = 100


    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(filename='work.log', level=level_debug, format=FORMAT)
    logging.info('Start program')
    logging.debug('Parcing arguments: %s %s %s', args.address, args.verbose, args.threads)

    main(args.threads)
