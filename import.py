import argparse
import psycopg2
import xml.etree.ElementTree as xml
from config import DB



def write_to_xml(Data_type, res):
    filename = Data_type.title() +'.xml'
    print(res)
    if Data_type=='cookies':
        sourse='url'
        D_type='cookie'
    elif Data_type=='urls':
        sourse='source url'
        D_type='url'

    root = xml.Element('z'+Data_type.title())
    '''appt = xml.Element(Data_type)    
    root.append(appt)'''

    cookie = xml.SubElement(root, D_type)
    cookie.text=res[0]+'\n'+'\t'
    if len(res)>1:
        srse = xml.SubElement(root, sourse)
        srse.text=res[1]+'\n'

    tree = xml.ElementTree(root)
    with open(filename, "ab") as fh:
        tree.write(fh)
        fh.write(b'\n')


def select_cookie_from_DB(type_of_file):
    conn = psycopg2.connect(host=args.address, user=DB['username'], password=DB['password'], dbname=DB['dbname'])   
    c = conn.cursor()

    c.execute('SELECT cookie,url FROM cookies;')
    res=c.fetchone()
    while res:
        write_to_xml('cookies',[res[0],res[1]])
        res=c.fetchone()
        
    return print('-------------------Cookies_writing_completed-------------------')

def select_url_from_DB(type_of_file, need_sourse=False):
    conn = psycopg2.connect(host=args.address, user=DB['username'], password=DB['password'], dbname=DB['dbname'])   
    c = conn.cursor()
    if need_sourse:
        c.execute('SELECT url,url_source FROM urls;')
        res=c.fetchone()
        while res:
            write_to_xml('urls',[res[0],res[1]])
            res=c.fetchone()
    else:
        c.execute('SELECT url FROM urls;')
        res=c.fetchone()
        while res:
            write_to_xml('urls',[res[0]])
            res=c.fetchone()
        
    return print('-------------------URL_writing_completed-------------------')

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