import requests
from bs4 import BeautifulSoup
import logging
import time
import pymysql
import random
import sys
import getopt
import datetime
import os

usr_agt = 'User-Agent:Mozilla/5.0 (X11; Linux x86_64)AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/51.0.2704.79 Chrome/51.0.2704.79 Safari/537.36'
accept = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
hdr = {}
hdr['User-Agent'] = usr_agt
hdr['Accept'] = accept

addr_body = "http://hz.58.com"
def_start_page = addr_body + "/puyan/chuzu/0/pn0"
record = {}

proxies = {}

logging.basicConfig(level=logging.INFO, format=' %(asctime)s - %(levelname)s - %(message)s')
#logging.basicConfig(level=logging.INFO, format=' %(asctime)s - %(levelname)s - %(message)s', \
#                     filename='rent.log', filemode='w')
'''
    database table columns
    +--------+-------------+------+-----+---------+-------+
    | Field  | Type        | Null | Key | Default | Extra |
    +--------+-------------+------+-----+---------+-------+
    | des    | varchar(50) | NO   | PRI | NULL    |       |
    | date   | varchar(10) | NO   |     | NULL    |       |
    | block  | varchar(10) | NO   |     | NULL    |       |
    | estate | varchar(10) | NO   |     | NULL    |       |
    | room   | varchar(10) | NO   |     | NULL    |       |
    | price  | varchar(10) | NO   |     | NULL    |       |
    | label  | varchar(5)  | YES  |     | NULL    |       |
    +--------+-------------+------+-----+---------+-------+
'''

def funnel_date_block_estate(obj):
    des = obj.find('a', {'class':'t'}).get_text()
    record['des'] = des
    if des is None:
        logging.warning("func:funnel_date_block_estate, des is None!!!")
        return None

    if (des.find('主卧') != -1):
        record['room'] = '1-1'
    elif (des.find('次卧') != -1):
        record['room'] = '1-2'
    elif (des.find('单间') != -1):
        record['room'] = '1-0'
    elif (des.find('隔断') != -1):
        record['room'] = '1-0'
    elif (des.find('床位') != -1):
        record['room'] = '1-0'


    bsobj = obj.find('p', {'class':'qj-renaddr'})
    if bsobj is None:
        logging.critical("func:funnel_date_block_estate, bsobj is None!!!")
        return None

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
    logging.info(renaddr[1].strip())
    m_estate = renaddr[1].strip().split(' ')
    logging.info(m_estate)
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
        exact label
    '''
    label = obj.find('p', {'class':'qj-rendp'}).label.get_text()[:-1]
    label = label + '-' + obj.find('p', {'class':'qj-rendp'}).a.get_text()
    logging.info('label:%s', label)
    record['label'] = label

def funnel_room_price(obj):
    obj_m = obj.find('b', {'class':'pri'})
    if obj_m is None:
        logging.warning('price is None!!!!')
        record['price'] = '0'
        record['room'] = '0'
        return
    price = obj_m.get_text() 
    record['price'] = price

    if record.get('room') is not None:
        return

    room = obj.find('span', {'class':'showroom'}).get_text()
    if room is None:
        logging.warning('room is None!!!!')
        record['room'] = '0'
        return

    if (room.find('主卧') != -1):
        record['room'] = '1-1'
        return
    elif (room.find('次卧') != -1):
        record['room'] = '1-2'
        return
    '''
        x室x厅x卫
    '''
    if (room.find('两')) != -1:
        record['room'] = '2'
        return
    elif (room.find('室')) != -1:
        pos = room.find('室')
        record['room'] = room[:pos]
        return
    record['room'] = room


def get_obj(site, tmout, proxy):

    try:
        r = requests.get(url=site, headers=hdr, proxies=proxy, timeout=tmout)
        r.raise_for_status()
    except Exception as err: 
        logging.info('func:get_obj, httperror:%s' %(err))
        return None 
  
    bsobj = BeautifulSoup(r.text, 'lxml')
    return bsobj

def parse_58(obj):
    bsobj = obj.find('table', {'class':'tbimg'})
    if bsobj is None:
        logging.critical("func:parse_58, Can't find table in html!!!")
        return None
    tr_list = bsobj.find_all('tr')
    for item in tr_list:
        if item is None:
            logging.warning('func:parse_58, item is None!!!')
            continue
       
        '''
            td_list[0]:don't care
            td_list[1]:des date block estate
            td_list[2]:room price label 
        '''
        td_list = item.find_all('td')
        '''
            empty list equal to False
        '''
        if not td_list:
            logging.critical("func:funnel_date_block_estate, td_list is None!!!")
            continue
        if len(td_list) < 3:
            logging.critical("func:funnel_date_block_estate, td_list is to short!, it may other item!!!")
            continue
        funnel_date_block_estate(td_list[1])
        funnel_room_price(td_list[2])
        
        logging.info(record)

        conn = pymysql.connect(host='localhost', user='proxy', passwd='proxy', db='proxydb', charset='utf8')
        cur = conn.cursor()
        cur.execute("INSERT IGNORE INTO 58rent (des, date, block, estate, room, price, label) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (record['des'], record['date'], record['block'], record['estate'], record['room'], record['price'], record['label']))
        conn.commit()
        cur.close()
        conn.close()

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

def check_ip(pool_list):
    pool_tmp = pool_list
    for each in pool_list:
        proxies['http'] = each
        logging.info('test ip activity:%s' %(each))
        obj = get_obj('http://www.baidu.com', 3.01, proxies)
        if obj is None:
            pool_tmp.remove(each)
            logging.info("proxy:%s is unavaluable" %(each))
    logging.info('ip pool:%s' %(pool_tmp))
    return pool_tmp

def init_pool():
    pool = []

    conn = pymysql.connect(host='104.128.81.253', user='proxy', passwd='proxy', db='proxydb', charset='utf8')
    cur = conn.cursor()
    cur.execute("SELECT * FROM proxy")
    
    for item in cur:
        pool.append('http://' + item[0] + ':' + item[1])
   
    pool_active = check_ip(pool)
    logging.info(pool_active)
    cur.close()
    conn.close()
    return pool_active 

def test():
    next_page = "http://hz.58.com/puyan/chuzu/0/pn0/"
    
    while True:
        logging.info('current page:%s' %(next_page))
#        proxies['http'] = 'http://218.205.80.4:80'
        proxies['http'] = None 

        '''
            step 1. 
                get obj, if resault is None, exit.
        '''
        obj = get_obj(next_page, None, proxies)
        if obj is None:
            logging.warning('get obj failed')
            exit(0)
        '''
            step 2.
                parse obj, exact data.
                if res is None, that mean no 'table' tag in html
                we should stop scrapy
        '''
        res = parse_58(obj)
        if res is None:
            logging.critical('func:main, cant find data in html!')
            exit(0)
        '''
            step 3.
                after handle current page, try to find next page. 
                if next page is None, we should stop scrapy
        '''
        next_page = addition_page(obj)
        if next_page is None:
            logging.critical('func:main, next_page is None')
            exit(0)
        '''
            step 4.
                anti-scrapy strategies.
                sleep
        '''
        t_sleep = random.randint(1,5)
        time.sleep(t_sleep)
#        time.sleep(1)

def main_loop(start_page, ip_pool):

    next_page = start_page

    for ip in ip_pool:
        proxies['http'] = ip
        rounds = 0

        while True:
            logging.info('proxies:%s page:%s' %(proxies, next_page))
            '''
                step 1. 
                    get obj, if resault is None, proxy may unavailabel.
                    so break while to choose next proxy
            '''
            obj = get_obj(next_page, 2, proxies)
            if obj is None:
                logging.warning('proxy:%s may unavailabel' %(ip))
                break
            '''
                step 2.
                    parse obj, exact data.
                    if res is None, that mean no 'table' tag in html, may 404 error
                    break, next proxy to try to scrapy                   
            '''
            res = parse_58(obj)
            if res is None:
                logging.critical('func:main, res is None.')
                break 
            '''
                step 3.
                    after handle current page, try to find next page. 
                    if next page is None, we should stop scrapy
            '''
            next_page = addition_page(obj)
            if next_page is None:
                logging.critical('func:main, next_page is None')
                return None 
            '''
                step 4.
                    anti-scrapy strategies.
                    sleep & change proxy
            '''
            t_sleep = random.randint(1,5)
            time.sleep(t_sleep)

    return True
#

if __name__ == '__main__':
    os.environ['TZ'] = 'PRC'
    time.tzset()
    run_back = False
    opts, args = getopt.getopt(sys.argv[1:], 'hpt', ['help', 'permanent', 'test',\
                                                    'sp='])
   
    start_page = def_start_page 
    for op,va in opts:
        if op in ['--sp']:
            start_page = va

    logging.info('start page:%s'%(start_page))
    for op,va in opts:      
        if op in ['-h', '--help']:
            print('unkonow')
        elif op in ['-p', '--permanent']:
            run_back = True
        elif op in ['-t', '--test']:
            test()

        while run_back is True:
            if (time.localtime().tm_hour > 0) and (time.localtime().tm_hour < 7):
                need_sleep = 7*60*60 - (time.localtime().tm_hour*60*60 + time.localtime().tm_min*60 + time.localtime().tm_sec)
                time.sleep(need_sleep)
            else: 
                ip_pool = init_pool()
                main_loop(start_page, ip_pool)

                curr_sec = (time.localtime().tm_min*60 + time.localtime().tm_sec)
                if curr_sec > 30*60:
                    need_sleep = 2*30*60 - curr_sec
                else:
                    need_sleep = 30*60 - curr_sec
                time.sleep(need_sleep)
 
    logging.critical('out of main')
    sys.exit(0)
