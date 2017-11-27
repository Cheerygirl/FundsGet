#!/usr/bin/python
# -*- coding: gb18030 -*-
__author__ = 'Cheery'

import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine,MetaData,TEXT,Table,Column
import mysql.connector
import csv
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

table_fund_series = Table('fund', meta,
    Column('fund_id', TEXT, nullable=True),
    Column('fund_date',TEXT, nullable=True),
    Column('fund_net_value', TEXT, nullable=True),
    Column('fund_cum_value', TEXT, nullable=True)
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
                    fund_data.to_sql(name='fund', con=engine, if_exists='append', index=False)
#                    fund_data.to_csv('todayvalue.csv',mode='a',encoding='gbk')
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

    def obtain_fund_todayvalue(self,html_cont, fundfeature):
        if (html_cont is None) or (fundfeature['fund_type'] is None):
            return
        soup = BeautifulSoup(html_cont, 'html.parser', from_encoding='gb18030')

        if fundfeature['fund_type'] == u'货币型':

            profit_dl = soup.find('dl', class_='dataItem01')
            profit_ratio_dl = soup.find('dl', class_='dataItem02')

            if (profit_dl is None) or (profit_ratio_dl is None):
                return
            net_update = profit_dl.find('p').text
            net_update_time = '2017-' + net_update[net_update.find(r'(') + 1:-1]
            net_value = profit_dl.find('dd').text
            cum_value = profit_ratio_dl.find('dd').text
        else:

            net_value_dl = soup.find('dl', class_='dataItem02')
            cum_value_dl = soup.find('dl', class_='dataItem03')

            if (net_value_dl is None) or (cum_value_dl is None) or (net_value_dl.find('p') is None):
                return
            net_update = net_value_dl.find('p').text
            net_update_time = net_update[net_update.find(r'(') + 1:-1]
            net_value_dd = net_value_dl.find('dd').text
            net_value = net_value_dd[0:5]
            cum_value_dd = cum_value_dl.find('dd').text
            cum_value = cum_value_dd[0:5]

        today_value = pd.Series([net_update_time, net_value, cum_value, fundfeature['fund_id']],
                                index=['fund_date', 'fund_net_value', 'fund_cum_value', 'fund_id'])
        return today_value

    def parse_url(self,url,fundfeature):
        fund_content = self.one_html_download(url)
        fund_value = self.obtain_fund_todayvalue(fund_content,fundfeature)
        today_value = pd.DataFrame(columns=['fund_date','fund_net_value','fund_cum_value','fund_id'])
        today_value = today_value.append(fund_value, ignore_index=True)
        return today_value

#    @staticmethod
#    def write_csv_head():
#        Handle_Url.del_csv_file()
#        with open('todayvalue.csv','ab') as wf:
#            head = ['fund_id','fund_date','fund_net_value','fund_cum_value']
#            writer = csv.write(wf)
#            writer.writerow(head)

#    def write_each_row_in_csv(self,text):
#        with open('todayvalue.csv','ab') as wf:
#            writer = csv.writer(wf)
#            writer.writerow(text)

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