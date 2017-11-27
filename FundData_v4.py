#!/usr/bin/python
# -*- coding: gb18030 -*-
__author__ = 'Cheery'

import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine,MetaData,TEXT,Table,Column
import mysql.connector
import time
import threading
from queue import *

########################read_data#######################
FeaTure = ('''SELECT fund_id,fund_name,fund_url,fund_type FROM fund.fundfeature;''')

cnx = mysql.connector.connect(user='root', password='mimi242526',host='localhost',
                              database='fund',use_unicode=True)
fund_feature = pd.DataFrame(pd.read_sql(FeaTure,con=cnx))

#####################

########################################################
CONSTR='mysql+mysqlconnector://root:mimi242526@localhost:3306/fund?charset=utf8'
engine=create_engine(CONSTR,echo=True)
meta = MetaData(bind=engine)

table_fund_series = Table('fundchange', meta,
    Column('fund_id', TEXT, nullable=True),
    Column('one_month',TEXT, nullable=True),
    Column('one_year', TEXT, nullable=True),
    Column('three_month', TEXT, nullable=True),
    Column('three_year', TEXT, nullable=True),
    Column('six_month', TEXT, nullable=True),
    Column('from_start', TEXT, nullable=True)
)

meta.create_all(engine)
#################################################################

class Handle_Url(threading.Thread):
    All_Funds = []
    def __init__(self,queue):
        super(Handle_Url, self).__init__()
        self.queue=queue

    def run(self):
        print('run in Parse_url')
        while True:
            if self.queue.empty():
                break
            else:
                fundfeature = self.queue.get()
                url = fundfeature.fund_url
                print(self.name+':'+'Begin parse %s now' %url)
                fund_data = self.parse_url(url,fundfeature)

                lock = threading.Lock()
                lock.acquire()
                try:
                    fund_data.to_sql(name='fundchange', con=engine, if_exists='append', index=False)
                finally:
                    lock.release()

    def one_html_download(self,url):
        if url is None:
            return None
        try:
            response = requests.get(url)
        except Exception as e:
            print("open the url failed, error:{}".format(e))
            return None

        if response.status_code != 200:
            return None
        return response.content

    def obtain_fund_change(self, html_cont):
        if (html_cont is None):
            return
        soup = BeautifulSoup(html_cont, 'html.parser', from_encoding='gb18030')

        change_div = soup.find('div', class_='dataOfFund')
        change_dd = change_div.find_all('dd')

        if (change_dd is None) or (change_div is None) or (len(change_dd)<8):
            return
        one_month_text = change_dd[1].text
        one_month = one_month_text[one_month_text.find(r'：') + 1:]
        one_year_text = change_dd[2].text
        one_year = one_year_text[one_year_text.find(r'：') + 1:]
        three_month_text = change_dd[4].text
        three_month = three_month_text[three_month_text.find(r'：') + 1:]
        three_year_text = change_dd[5].text
        three_year = three_year_text[three_year_text.find(r'：') + 1:]
        six_month_text = change_dd[7].text
        six_month = six_month_text[six_month_text.find(r'：') + 1:]
        from_start_text = change_dd[8].text
        from_start = from_start_text[from_start_text.find(r'：') + 1:]

        fund_change = pd.Series([one_month, one_year, three_month, three_year, six_month, from_start],
                                index=['one_month', 'one_year', 'three_month', 'three_year', 'six_month', 'from_start'])
        return fund_change

    def parse_url(self,url,fundfeature):
        fund_content = self.one_html_download(url)
        fund_value = self.obtain_fund_change(fund_content)
        fund_change = pd.DataFrame(columns=['one_month', 'one_year', 'three_month', 'three_year', 'six_month', 'from_start'])
        fund_change = fund_change.append(fund_value, ignore_index=True)
        fund_change['fund_id'] = fundfeature['fund_id']
        return fund_change

#################################################################
def main():
    start=time.time()
    # main code
    queue = Queue()
    threads=[]
    # get all the funds info
    funds = fund_feature
    # put all the fund_text info queue
    for i in range(len(funds)):
        queue.put(funds.iloc[i])
    #create the multi-thread
    for i in range(4):
        c=Handle_Url(queue)
        threads.append(c)
        c.start()
    #wait for all thread finish
    for t in threads:
        t.join()
    print('Cost:{0} seconds'.format(time.time()-start))

main()


