#key-desc-date-block-estate-room-price-lable
import requests
from bs4 import BeautifulSoup
import logging
import time
import pymysql

usr_agt = 'User-Agent:Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/51.0.2704.79 Chrome/51.0.2704.79 Safari/537.36'
hdr = {}
hdr['User-Agent'] = usr_agt

addr_body = "http://hz.58.com"
init_addr = addr_body + "/puyan/zufang/0/pn0"
record = {}

logging.basicConfig(level=logging.INFO, format=' %(asctime)s - %(levelname)s - %(message)s')
#logging.basicConfig(level=logging.INFO, format=' %(asctime)s - %(levelname)s - %(message)s', \
#                     filename='rent.log', filemode='w')

def funnel_date_block_estate(obj):
    desc = obj.find('a', {'class':'t'}).get_text()
    record['desc'] = desc
    if desc is None:
        logging.warning("func:funnel_date_block_estate, desc is None!!!")

    bsobj = obj.find('p', {'class':'qj-renaddr'})
    if bsobj is None:
        logging.critical("func:funnel_date_block_estate, bsobj is None!!!")
        return

    renaddr = bsobj.get_text().strip().split('\r\n')
    '''
        [0] stored block
        [1] stored estate
        [2] stored date
    '''
    block = renaddr[0].strip()
    record['block'] = block
    '''
        extract estate from string like:'- estate_name\n/'
        1.split(' ')
        2.remove last 4 character '租房\n/'
    '''
    m_estate = renaddr[1].strip().split(' ')
    estate = m_estate[1].strip()[:-4]
    record['estate'] = estate
    '''
        convert to xxxx-xx-xx
    '''
    date_m = renaddr[2].strip()
    m_d = date_m.split('-')
    today = time.strftime('%Y-%02m-%02d',time.localtime(time.time()))
    today_m = today.split('-')
    cur_year = today_m[0]
    if len(m_d) == 1:
        date = today
    if len(m_d) == 2:
        date = cur_year + '-' + date_m 
    record['date'] = date
    '''
        exact lable
    '''
    label = obj.find('p', {'class':'qj-rendp'}).label.get_text()[:-1]
    record['label'] = label

def funnel_room_price(obj):
    obj_m = obj.find('b', {'class':'pri'})
    if obj_m is None:
        logging.critical('price is None!!!!')
        return None
    price = obj_m.get_text() 
    record['price'] = price

    room = obj.find('span', {'class':'showroom'}).get_text()
    if room is None:
        logging.critical('room is None!!!!')
    record['room'] = room
    logging.info(record)

def get_obj(site, tmout):
    print("site:%s, tmout:%s" %(site, tmout))
    try:
        r = requests.get(url=site, headers=hdr, timeout=tmout)
        r.raise_for_status()
    except Exception as err:
        logging.critical('open %s failed, reason:%s' %(site, err))
        return None
 
    bsobj = BeautifulSoup(r.text, 'lxml')
    return bsobj

def parse_58(obj):
    bsobj = obj.find('table', {'class':'tbimg'})
    if bsobj is None:
        logging.critical("Can't find table")
        return None
    tr_list = bsobj.find_all('tr')
    for item in tr_list:
        if item is None:
            logging.warning('func:parse_58, item is None!!!')
            continue
       
        '''
            td_list[0]:don't care
            td_list[1]:desc date block estate
            td_list[2]:room price label 
        '''
        td_list = item.find_all('td')
        '''
            empty list equal to False
        '''
        if not td_list:
            logging.critical("func:funnel_date_block_estate, td_list is None!!!")
            continue
        funnel_date_block_estate(td_list[1])
        funnel_room_price(td_list[2])
        record.clear()
    return True

def addition_page(obj):
    obj = obj.body.find('div', {'class':'pager'})
    if obj is None:
        print('func:addition_page, obj is None')
        return None
    next_tag = obj.find('a', {'class':'next'})['href']
    if next_tag is None:
        print('func:addition_page, next_tag is None')
        return None

    next_page = addr_body + next_tag
    return next_page

def init_pool():
    conn = pymysql.connect(host='104.128.81.253', user='proxy', passwd='proxy', db='proxydb', charset='utf8')
    cur = conn.cursor()

    cur.execute("SELECT * FROM proxy WHERE disconntm = 0")
    return cur

if __name__ == '__main__':
    cur = init_pool()
    for item in cur:
        print(item)
'''
    page = init_addr
    obj = get_obj(init_addr, 3)
    while obj is not None:
        res = parse_58(obj)
        if res is None:
            logging.critical('func:main, res is None')
        next_page = addition_page(obj)
        if next_page is None:
            logging.critical('func:main, next_page is None')
            break
        sleep(1)
        obj = get_obj(next_page, 3) 
    logging.info('out of main')
    exit(0)
    '''


