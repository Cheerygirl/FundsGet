import requests
from bs4 import BeautifulSoup
import re

url = 'http://fund.eastmoney.com/allfund.html#0'

def html_download(url):
    if url is None:
        return None
    try:
        response = requests.get(url)
    except Exception as e:
        print "open the url failed, error:{}".format(e)
        return None

    if response.status_code != 200:
        return None
    return response.content

UrlContent = html_download(url)

def html_extract_content(html_cont):
    funds_text = []
    if html_cont is None:
        return
    soup = BeautifulSoup(html_cont,'html.parser',from_encoding='gb18030')
    '''get all the fund id'''
    title_node = soup.title
    print title_node.getText()

    uls = soup.find_all('ul',class_='num_right')
    for ul in uls:
        for each in ul.find_all('li'):
            #print each
            li_list = each.find_all('a')
            fund_info_dict = {'fund_id':'',
                              'fund_name':'',
                              'fund_url':''}
            if len(li_list)>1:
                fund = li_list[0].text
                fund_id = re.findall(r'\d+',fund)[0]
                fund_url = li_list[0].attrs['href']
                fund_name = fund[fund.find(ur'）') + 1:].encode('utf8')
                fund_info_dict['fund_id'] = fund_id
                fund_info_dict['fund_name'] = fund_name
                fund_info_dict['fund_url'] = fund_url
                funds_text.append(fund_info_dict)
    return funds_text

Funds = html_extract_content(UrlContent)

def one_html_download(url):
    if url is None:
        return None
    try:
        response = requests.get(url)
    except Exception as e:
        print "open the url failed, error:{}".format(e)
        return None

    if response.status_code != 200:
        return None
    return response.content

def one_html_decode(html_cont):
    funds_text = []
    if html_cont is None:
        return
    soup = BeautifulSoup(html_cont,'html.parser',from_encoding='gb18030')
    '''get all the fund id'''
    title_node = soup.title
    print title_node.getText()

    uls = soup.find_all('ul',class_='num_right')
    for ul in uls:
        for each in ul.find_all('li'):
            #print each
            li_list = each.find_all('a')
            fund_info_dict = {'fund_id':'',
                              'fund_name':'',
                              'fund_url':''}
            if len(li_list)>1:
                fund = li_list[0].text
                fund_id = re.findall(r'\d+',fund)[0]
                fund_url = li_list[0].attrs['href']
                fund_name = fund[fund.find(ur'）') + 1:].encode('utf8')
                fund_info_dict['fund_id'] = fund_id
                fund_info_dict['fund_name'] = fund_name
                fund_info_dict['fund_url'] = fund_url
                funds_text.append(fund_info_dict)
    return funds_text

for fund in Funds:
    fund_id = fund['fund_id']
    fund_name = fund['fund_name']
    fund_url = fund['fund_url']
    fund_content = one_html_download(fund_url)

def parse_url(self,url):
    html_content = self.html_download(url)
    return self.html_decode(html_content)

def write_csv_head():
    Handle_Url.del_csv_file()
    with open(CSV_File, 'ab') as wf:
        head = ['fund_id', 'fund_name', 'one_month',
                'one_year', 'three_month', 'three_year',
                'six_month', 'from_start']
        writer = csv.writer(wf)
        writer.writerow(head)

def write_each_row_in_csv(self,text):
    with open(CSV_File, 'ab') as wf:
        writer = csv.writer(wf)
        writer.writerow(text)

lock = threading.Lock()
lock.acquire()
try:
    self.write_each_row_in_csv(fund_data)
finally:
    lock.release()

from Queue import Queue
queue = Queue()
threads = []

#get all the funds info
funds = url_download.get_all_funds_dict()
#put all the fund_text info queue
for fund_text in funds:
    queue.put(fund_text)

#create the multi-thread
for i in range(8):
    c=Handle_Url(queue)
    threads.append(c)
    c.start()
#wait for all thread finish
for t in threads:
    t.join()

import time
start=time.time()
# main code
print 'Cost:{0} seconds'.format(time.time()-start)