#!/usr/bin/python
# -*- coding: gb18030 -*-
__author__ = 'Cheery'

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from sqlalchemy import create_engine,MetaData,TEXT,Integer,Table,Column,ForeignKey

CONSTR='mysql+mysqlconnector://root:mimi242526@localhost:3306/fund?charset=utf8'
engine=create_engine(CONSTR,echo=True)
meta = MetaData(bind=engine)

table_fund_feature = Table('fundfeature', meta,
    Column('fund_id', TEXT, nullable=True),
    Column('fund_name', TEXT, nullable=True),
    Column('fund_url', TEXT, nullable=True),
    Column('fund_risk', TEXT, nullable=True),
    Column('fund_type', TEXT, nullable=True),
    Column('fund_scale', TEXT, nullable=True),
    Column('fund_operator', TEXT, nullable=True),
    Column('fund_setupday', TEXT, nullable=True),
    Column('fund_company', TEXT, nullable=True),
    Column('fund_rate', TEXT, nullable=True)
)

table_fund_series = Table('fund', meta,
    Column('fund_id', TEXT, nullable=True),
    Column('fund_date',TEXT, nullable=True),
    Column('fund_net_value', TEXT, nullable=True),
    Column('fund_cum_value', TEXT, nullable=True)
)

meta.create_all(engine)

url = 'http://fund.eastmoney.com/allfund.html#0'

def html_download(url):
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

UrlContent = html_download(url)

def obtain_all_funds(html_cont):
    funds_text = []
    if html_cont is None:
        return
    soup = BeautifulSoup(html_cont,'html.parser',from_encoding='gb18030')
    '''get all the fund id'''
    title_node = soup.title
    print(title_node.getText())

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
                fund_name = fund[fund.find(r'）') + 1:].encode('utf8')
                fund_info_dict['fund_id'] = fund_id
                fund_info_dict['fund_name'] = fund_name
                fund_info_dict['fund_url'] = fund_url
                funds_text.append(fund_info_dict)
    return funds_text

Funds = obtain_all_funds(UrlContent)

def one_html_download(url):
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

def obtain_fund_series(fund_id):
    data_return = []

    # http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=000001&page=1&per=1000&sdate=&edate=&rt=0.21058104961666246
    url = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=%s&page=1&per=2000'
    content = one_html_download(url % fund_id)
    if content is None:
        return pd.DataFrame(data_return, columns=['date', 'net_value', 'cum_value','fund_id'])

    soup = BeautifulSoup(content, 'lxml')
    tbody = soup.find('tbody')
    trs = tbody.find_all('tr')

    if len(trs)<=3:
        print(trs[0].text)
        return pd.DataFrame(data_return, columns=['date', 'net_value', 'cum_value','fund_id'])

    for tr in trs:
        row_of_data = []
        tds = tr.find_all('td')
        date = tds[0].text
        net = tds[1].text
        cum = tds[2].text
        row_of_data.append(date)
        row_of_data.append(net)
        row_of_data.append(cum)
        data_return.append(row_of_data)
    data_return = pd.DataFrame(data_return,columns=['fund_date', 'fund_net_value', 'fund_cum_value'])
    data_return['fund_id'] = fund_id
    return pd.DataFrame(data_return)

for fund in Funds[6870:]:
    fund_id = fund['fund_id']
    fund_url = fund['fund_url']
    fund_content = one_html_download(fund_url)
    print(fund_id)
    fund_series = obtain_fund_series(fund_id)
#    fund_series.to_csv('FundSeries.csv',mode='a',encoding='gbk',header=False)
    fund_series.to_sql(name='fund', con=engine, if_exists='append', index=False,chunksize=10000)

def obtain_fund_feature(html_cont,fund):
    if html_cont is None:
        return pd.Series([None,None,None,None,None,None,None,fund['fund_id'],fund['fund_name'],fund['fund_url']],
                         index=['fund_type','fund_risk','fund_scale','fund_operator','fund_setupday','fund_company','fund_rate','fund_id','fund_name','fund_url'])
    soup = BeautifulSoup(html_cont, 'html.parser', from_encoding='gb18030')
    type_divs = soup.find('div',class_='infoOfFund')

    if type_divs is None:
        return
    tds = type_divs.find_all('td')

    td1 = tds[0].text
    if len(td1) <= 8:
        type1 = td1[(td1.find(r'：')+1):]
        type2 = u'低风险'
    else:
        type1 = td1[(td1.find(r'：')+1):(td1.find(r' '))]
        type2 = td1[(td1.find(r'|  ')+1):]

    td2 = tds[1].text
    type3 = td2[(td2.find(r'：')+1):(td2.find(r'（'))]
    type4 = td2[(td2.find(r'（')+1):-1]
    td3 = tds[2].text
    type4 = td3[(td3.find(r'：') + 1):]
    td4 = tds[3].text
    type5 = td4[(td4.find(r'：') + 1):]
    td5 = tds[4].text
    type6 = td5[(td5.find(r'：') + 1):]
    type7 = tds[5].find('div')['class'][0]

    fund_feature = pd.Series([type1,type2,type3,type4,type5,type6,type7,fund['fund_id'],fund['fund_name'],fund['fund_url']],
                             index=['fund_type','fund_risk','fund_scale','fund_operator','fund_setupday','fund_company','fund_rate','fund_id','fund_name','fund_url'])
    return fund_feature

def obtain_fund_todayvalue(html_cont,fund_id,fund_type=None):
    if (html_cont is None) or (fund_feature is None):
        return
    soup = BeautifulSoup(html_cont,'html.parser',from_encoding='gb18030')

    if fund_type == u'货币型':

        profit_dl = soup.find('dl', class_='dataItem01')
        profit_ratio_dl = soup.find('dl', class_='dataItem02')

        if (profit_dl is None) or (profit_ratio_dl is None):
            return
        net_update = profit_dl.find('p').text
        net_update_time = '2017-'+net_update[net_update.find(r'(') + 1:-1]
        net_value = profit_dl.find('dd').text
        cum_value = profit_ratio_dl.find('dd').text
    else:

        net_value_dl = soup.find('dl',class_='dataItem02')
        cum_value_dl = soup.find('dl',class_='dataItem03')

        if (net_value_dl is None) or (cum_value_dl is None) or (net_value_dl.find('p') is None):
            return
        net_update = net_value_dl.find('p').text
        net_update_time = net_update[net_update.find(r'(') + 1:-1]
        net_value_dd = net_value_dl.find('dd').text
        net_value = net_value_dd[0:5]
    #    net_value_ratio = net_value_dd[6:]
    #    net_series_link = net_value_dl.find('span',class_='sp01').find('a').get('href')
        cum_value_dd = cum_value_dl.find('dd').text
        cum_value = cum_value_dd[0:5]
    #    cum_series_link = cum_value_dl.find('span',class_='sp01').find('a').get('href')

    today_value = pd.Series([net_update_time,net_value,cum_value,fund_id],
                             index=['fund_date','fund_net_value','fund_cum_value','fund_id'])
    return today_value

for fund in Funds[5358:]:
    fund_id = fund['fund_id']
    fund_url = fund['fund_url']
    fund_content = one_html_download(fund_url)
    print(fund_id)

    FundsFeature = pd.DataFrame(columns=['fund_type','fund_risk','fund_scale','fund_operator','fund_setupday','fund_company','fund_rate','fund_id','fund_name','fund_url'])
    fund_feature = obtain_fund_feature(fund_content,fund)
    FundsFeature = FundsFeature.append(fund_feature,ignore_index=True)
    FundsFeature.to_sql(name='fundfeature', con=engine, if_exists='append', index=False)

#    TodayValue = pd.DataFrame(columns=['fund_date','fund_net_value','fund_cum_value','fund_id'])
#    today_value = obtain_fund_todayvalue(fund_content,fund_id,fund_feature['fund_type'])
#    TodayValue = TodayValue.append(today_value,ignore_index=True)
#    TodayValue.to_sql(name='fund', con=engine, if_exists='append', index=False)
