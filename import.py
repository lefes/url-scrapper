import argparse
import psycopg2
import csv
from config import DB

EXTENTIONS={'xml':'.xml','csv':'.csv', 'sql':'.sql', 'html':'.html'}

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
        f.write('<zCookies>\n\t')
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
        

def select_cookie_from_DB(type_of_file):
    conn = psycopg2.connect(host=args.address, user=DB['username'], password=DB['password'], dbname=DB['dbname'])   
    c = conn.cursor()

    c.execute('SELECT cookie,url FROM cookies;')
    res=c.fetchone()
    filename='Cookies'+EXTENTIONS[type_of_file]
    if type_of_file=='xml':
        write_to_xml(filename,c)

    elif type_of_file=='csv':
        write_to_csv(filename,c)




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

    select_url_from_DB('xml',need_sourse=True)
    if args.url==1:
        select_url_from_DB(args.extention,need_sourse=False)
    elif args.url>1:
        select_url_from_DB(args.extention,need_sourse=True)
    
    if args.cookie>1:
        select_cookie_from_DB(args.extention)