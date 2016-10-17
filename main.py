#court_list['临江花园', '碧水豪园', '钱塘山水', '钱江湾花园',
#            '超级星期天', '贺田尚城', '官邸国际', '逸天广场',
#            '天鸿君邑', '积家', '江畔云庐', '江南文苑', '银爵世纪',
#            '金盛曼城', '星汇荣邸']


#日期-小区名-出租条件(一套(10/20/30/40/50)/主卧(11)/次卧()12)-价格

import requests
from bs4 import BeautifulSoup
import logging

usr_agt = 'User-Agent:Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/51.0.2704.79 Chrome/51.0.2704.79 Safari/537.36'
hdr = {}
hdr['User-Agent'] = usr_agt

site_58 = "http://hz.58.com/puyan/zufang/0/"

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
