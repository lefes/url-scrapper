import psycopg2
import sys
import datetime
from config import DB

def flush_db(con):
    cur = con.cursor()
    cur.execute('DROP DATABASE IF EXISTS {};'.format(DB['dbname']))

def create_db(con):
    cur = con.cursor()
    cur.execute('CREATE DATABASE {};'.format(DB['dbname']))
    con.close()
    con = psycopg2.connect(host=sys.argv[1], user=DB['username'], password=DB['password'], dbname=DB['dbname'])
    con.autocommit = True
    cur = con.cursor()
    cur.execute("""

        CREATE TABLE cookies (
            id SERIAL UNIQUE,
            url varchar(255)  NOT NULL,
            cookie text  NOT NULL,
            CONSTRAINT cookies_pk PRIMARY KEY (id)
        );

        CREATE TABLE urls (
            id SERIAL UNIQUE,
            url varchar(255)  NOT NULL,
            url_source varchar(255)  NOT NULL,
            datetime timestamp  NOT NULL,
            checked integer  NOT NULL,
            CONSTRAINT url UNIQUE (url) NOT DEFERRABLE  INITIALLY IMMEDIATE,
            CONSTRAINT urls_pk PRIMARY KEY (id)
        );

        ALTER TABLE cookies ADD CONSTRAINT cookies_urls
            FOREIGN KEY (url)
            REFERENCES urls (url)
            ON DELETE  CASCADE 
            ON UPDATE  CASCADE 
            NOT DEFERRABLE 
            INITIALLY IMMEDIATE
        ;""")
    cur.execute("INSERT INTO urls (url, url_source, datetime, checked) VALUES ('https://google.com', 'Manual', '{}', 0);".format(datetime.datetime.now()))
    con.close()

if __name__ == "__main__":
    con = psycopg2.connect(host=sys.argv[1], user=DB['username'], password=DB['password'], dbname='postgres')
    con.autocommit = True
    flush_db(con)
    create_db(con)