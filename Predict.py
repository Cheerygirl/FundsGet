#!/usr/bin/python
# -*- coding: gb18030 -*-
__author__ = 'Cheery'

import os
os.environ['TF_CPP_MIN_LOG_LEVEL']='2'
import tensorflow as tf
from keras.models import Sequential
from keras.layers import Dense,LSTM
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error

import mysql.connector
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt

########################read_data#######################
cnx = mysql.connector.connect(user='chaoge', password='123456',host='114.115.206.10',
                              database='qq',use_unicode=True)

stocks = pd.read_csv('stocks.csv',encoding='utf8',header=None)

def create_dataset(ts,look_back=1):
    dataX,dataY = [], []
    for i in range(len(ts)-look_back-1):
        a = ts[i:(i+look_back)]
        dataX.append(a)
        dataY.append(ts[(i + look_back)])
    return np.array(dataX),np.array(dataY)

def LSTMModel(ts,look_back=1):
    # scale date
    scaler = MinMaxScaler(feature_range=(0, 1))
    dataset = scaler.fit_transform(ts)

    # split into train and test sets
    train_size = int(len(ts) * 0.67)
    test_size = len(ts) - train_size
    train, test = dataset[0:train_size], dataset[train_size:len(dataset)]

    # use this function to prepare the train and test datasets for modeling
    trainX, trainY = create_dataset(train, look_back)
    testX, testY = create_dataset(test, look_back)

    # reshape input to be [samples(样本数), time steps, features(look_back)]
    trainX = np.reshape(trainX, (trainX.shape[0], 1, trainX.shape[1]))
    testX = np.reshape(testX, (testX.shape[0], 1, testX.shape[1]))

    # create and fit the LSTM network
    model = Sequential()
    model.add(LSTM(4, input_shape=(1, look_back)))
    model.add(Dense(1))
    model.compile(loss='mean_squared_error', optimizer='adam')
    model.fit(trainX, trainY, epochs=100, batch_size=1, verbose=2)

    # make predictions
    trainPredict = model.predict(trainX)
    testPredict = model.predict(testX)

    # invert predictions
    trainPredict = scaler.inverse_transform(trainPredict)
    trainY = scaler.inverse_transform([trainY])
    testPredict = scaler.inverse_transform(testPredict)
    testY = scaler.inverse_transform([testY])

    trainScore = math.sqrt(mean_squared_error(trainY[0], trainPredict[:,0]))
    print('Train Score: %.2f RMSE' % (trainScore))
    testScore = math.sqrt(mean_squared_error(testY[0], testPredict[:,0]))
    print('Test Score: %.2f RMSE' % (testScore))

    # shift train predictions for plotting
    trainPredictPlot = np.empty_like(dataset)
    trainPredictPlot[:] = np.nan
    trainPredictPlot[look_back:len(trainPredict)+look_back] = trainPredict[:,0]

    # shift test predictions for plotting
    testPredictPlot = np.empty_like(dataset)
    testPredictPlot[:] = np.nan
    testPredictPlot[len(trainPredict)+(look_back*2)+1:len(dataset)-1] = testPredict[:,0]

    # plot baseline and predictions
    plt.plot(scaler.inverse_transform(dataset))
    plt.plot(trainPredictPlot)
    plt.plot(testPredictPlot)
    plt.show()

    return np.append(trainPredict,testPredict)


for i in range(len(stocks)):
    sql = 'SELECT * FROM qq.'+stocks.iloc[i]+';'
    stock = pd.DataFrame(pd.read_sql((sql.values[0]),con=cnx))
    stock.rename(columns={u'日期':'stock_date',u'股票代码':'stock_id',u'名称':'stock_name',
                          u'收盘价':'close_price',u'最高价':'high_price',u'最低价':'low_price',
                          u'开盘价':'open_price',u'涨跌幅':'price_limit',u'换手率':'turnover_rate',
                          u'成交量':'volumn',u'主力净流入万':'capital_inflow',u'主力净流入率':'capital_inflow_rate',
                          u'主力罗盘':'capital_compass',u'净流入万':'inflow',u'净流入率':'inflow_rate',
                          u'待定':'TBD',u'行业净流入率':'industry_inflow_rate',u'行业净流入量':'industry_inflow',
                          u'净大单':'big_one',u'净小单':'small_one',u'净散单':'bulk_single',
                          u'净超大单':'super_one',u'大单':'big_sum',u'小单':'small_list',
                          u'散单':'bulk_sum',u'超大单':'min_one'},inplace=True)
    predit_value = LSTMModel(stock.close_price,3)

