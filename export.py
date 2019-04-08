import argparse
import psycopg2
import csv
from config import DB

EXTENTIONS={'xml':'.xml','csv':'.csv', 'sql':'.sql', 'html':'.html'}

def write_to_sql(filename, c):
    with open(filename,'w') as f:
        f.write('''--
-- PostgreSQL database dump
--

-- Dumped from database version 11.2
-- Dumped by pg_dump version 11.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

ALTER TABLE IF EXISTS ONLY public.cookies DROP CONSTRAINT IF EXISTS cookies_urls;
ALTER TABLE IF EXISTS ONLY public.urls DROP CONSTRAINT IF EXISTS urls_pk;
ALTER TABLE IF EXISTS ONLY public.urls DROP CONSTRAINT IF EXISTS url;
ALTER TABLE IF EXISTS ONLY public.cookies DROP CONSTRAINT IF EXISTS cookies_pk;
ALTER TABLE IF EXISTS public.urls ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.cookies ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE IF EXISTS public.urls_id_seq;
DROP TABLE IF EXISTS public.urls;
DROP SEQUENCE IF EXISTS public.cookies_id_seq;
DROP TABLE IF EXISTS public.cookies;
SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: cookies; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.cookies (
    id integer NOT NULL,
    url character varying(255) NOT NULL,
    cookie text NOT NULL
);


ALTER TABLE public.cookies OWNER TO postgres;

--
-- Name: cookies_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.cookies_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.cookies_id_seq OWNER TO postgres;

--
-- Name: cookies_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.cookies_id_seq OWNED BY public.cookies.id;


--
-- Name: urls; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.urls (
    id integer NOT NULL,
    url character varying(255) NOT NULL,
    url_source character varying(255) NOT NULL,
    datetime timestamp without time zone NOT NULL,
    checked integer NOT NULL
);


ALTER TABLE public.urls OWNER TO postgres;

--
-- Name: urls_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.urls_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.urls_id_seq OWNER TO postgres;

--
-- Name: urls_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.urls_id_seq OWNED BY public.urls.id;


--
-- Name: cookies id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cookies ALTER COLUMN id SET DEFAULT nextval('public.cookies_id_seq'::regclass);


--
-- Name: urls id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.urls ALTER COLUMN id SET DEFAULT nextval('public.urls_id_seq'::regclass);


--
-- Data for Name: urls; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.urls (id, url, url_source, datetime, checked) FROM stdin;''')
        res=c.fetchone()
        while res:
            print(res)
            try:
                s=res[0]+'                    '+res[1]
                s=('%r'%s)[1:-1]
                s+='\n'
                f.write(s)
            except:
                pass
            res=c.fetchone()
        f.write('''--
-- Name: cookies_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.cookies_id_seq', 326254, true);


--
-- Name: urls_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.urls_id_seq', 8461769, true);


--
-- Name: cookies cookies_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cookies
    ADD CONSTRAINT cookies_pk PRIMARY KEY (id);


--
-- Name: urls url; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.urls
    ADD CONSTRAINT url UNIQUE (url);


--
-- Name: urls urls_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.urls
    ADD CONSTRAINT urls_pk PRIMARY KEY (id);


--
-- Name: cookies cookies_urls; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cookies
    ADD CONSTRAINT cookies_urls FOREIGN KEY (url) REFERENCES public.urls(url) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--
''')
        


def write_to_csv(filename, stream):
    with open(filename,'w') as f:
        writer = csv.writer(f,delimiter=';')
        res=stream.fetchone()
        if filename=='Cookies.csv':
            writer.writerow(('cookie','source url'))
        elif len(res)>1 and filename=='Urls.csv':
            writer.writerow(('url','source url;'))
        else:
            writer.writerow(('url'))
        while res:
            print(res)
            writer.writerow(res)
            res=stream.fetchone()


def write_to_xml(filename, stream):
    data = filename[:-5]
    source='source_url'
    with open(filename,'w') as f:
        f.write('<z'+data.capitalize()+'>\n\t')
        res=stream.fetchone()
        while res:
            print(res)
            s='<'+data+'>'+res[0]+'</'+data
            if len(res)>1:
                so='<'+source+'>'+res[1]+'</'+source+'>\n\t'  
            s=('%r'%s)[1:-1]
            s+='>\n\t'
            try:
                f.write(s)
                if len(res)>1:
                    f.write(so)
            except:
                pass
            res=stream.fetchone()
        f.write('\n</z'+data.capitalize()+'>\n\t')
        

def select_cookie_from_DB(type_of_file):
    conn = psycopg2.connect(host=args.address, user=DB['username'], password=DB['password'], dbname=DB['dbname'])   
    c = conn.cursor()

    c.execute('SELECT cookie,url FROM cookies;')
    filename='Cookies'+EXTENTIONS[type_of_file]
    if type_of_file=='xml':
        write_to_xml(filename,c)

    elif type_of_file=='csv':
        write_to_csv(filename,c)
    elif type_of_file=='sql':
        print('writing to sql for cookies are not supporting')


def select_url_from_DB(type_of_file, need_sourse=False):
    conn = psycopg2.connect(host=args.address, user=DB['username'], password=DB['password'], dbname=DB['dbname'])   
    c = conn.cursor()
    if need_sourse:
        c.execute('SELECT url,url_source FROM urls;')
    else:
        c.execute('SELECT url FROM urls;')
    if type_of_file=='csv':
        write_to_csv('Urls.csv',c)
    elif type_of_file=='xml':
        write_to_xml('Urls.xml',c)
    elif type_of_file=='sql':
        write_to_sql('Urls.sql',c)

if __name__ == '__main__':
    parser=argparse.ArgumentParser(description='Import/Export info from DB to some files')
    parser.add_argument('-e', '--extention', type=str,help='extention of result file')
    parser.add_argument('-u', '--url', help='Reading cout of -u: 1 - only url without source url, 2 - url and souce url',
                        action='count', default=0)
    parser.add_argument('-c', '--cookie', help='',
                        action='count', default=0)
    parser.add_argument('-a', '--address', type=str, help='default ip address is 127.0.0.1',
                        default='127.0.0.1', metavar='')
    args=parser.parse_args()

    select_url_from_DB('sql',need_sourse=True)
    if args.url==1:
        select_url_from_DB(args.extention,need_sourse=False)
    elif args.url>1:
        select_url_from_DB(args.extention,need_sourse=True)
    
    if args.cookie>=1:
        select_cookie_from_DB(args.extention)