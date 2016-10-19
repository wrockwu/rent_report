#court_list['临江花园', '碧水豪园', '钱塘山水', '钱江湾花园',
#            '超级星期天', '贺田尚城', '官邸国际', '逸天广场',
#            '天鸿君邑', '积家', '江畔云庐', '江南文苑', '银爵世纪',
#            '金盛曼城', '星汇荣邸']


#desc-date-block-estate-room-price-lable
import requests
from bs4 import BeautifulSoup
import logging
import time

usr_agt = 'User-Agent:Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/51.0.2704.79 Chrome/51.0.2704.79 Safari/537.36'
hdr = {}
hdr['User-Agent'] = usr_agt

site_58 = "http://hz.58.com/puyan/zufang/0/pn0"

logging.basicConfig(level=logging.INFO, format=' %(asctime)s - %(levelname)s - %(message)s')
#logging.basicConfig(level=logging.INFO, format=' %(asctime)s - %(levelname)s - %(message)s', \
#                     filename='rent.log', filemode='w')

def funnel_date_block_estate(obj):
    desc = obj.find('a', {'class':'t'}).get_text()
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
    '''
        extract estate from string like:'- estate_name\n/'
        1.split(' ')
        2.remove last 4 character '租房\n/'
    '''
    m_estate = renaddr[1].strip().split(' ')
    estate = m_estate[1].strip()[:-4]
    '''
        convert to xxxx-xx-xx
    '''
    date_m = renaddr[2].strip()
    m_d = date_m.split('-')
    today = time.strftime('%Y-%02m-%02d',time.localtime(time.time()))
    today_m = today.split('-')
    cur_year = today_m[0]
    print(m_d)
    if len(m_d) == 1:
        date = today
    if len(m_d) == 2:
        date = cur_year + '-' + date_m 
    logging.info('desc:%s date:%s block:%s estate:%s' %(desc, date, block, estate))

def funnel_room_price(obj):
    print(obj.find('b', {'class':'pri'}))
    price = obj.find('b', {'class':'pri'}).get_text()
    print(obj.find('span', {'class':'showroom'}))
    room = obj.find('span', {'class':'showroom'}).get_text()

    logging.info('price:%s room:%s' %(price, room))





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
        print("Can't find table")
        return
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
#        court = renaddr[1].lstrip()
#        print(court)
#        print("hello next")

#    bsobj = bsobj
#    print(bsobj)

#    db_conn()
#    for child in bsobj.find_all('tr'):
#        ip = child.td.get_text()
#        port = child.td.next_sibling.next_sibling.get_text()
    '''
        set 'prot' to http manually 
    '''
#    prot = 'http'
#    sql = proxy_insert 
#    db_update(sql, (ip, port, "NULL", prot, 0))
#    db_close()

if __name__ == '__main__':
    
    obj = get_obj(site_58, 3)
    parse_58(obj)
    #print(obj)
