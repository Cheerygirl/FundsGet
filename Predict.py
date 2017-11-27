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

    # reshape input to be [samples(������), time steps, features(look_back)]
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
    stock.rename(columns={u'����':'stock_date',u'��Ʊ����':'stock_id',u'����':'stock_name',
                          u'���̼�':'close_price',u'��߼�':'high_price',u'��ͼ�':'low_price',
                          u'���̼�':'open_price',u'�ǵ���':'price_limit',u'������':'turnover_rate',
                          u'�ɽ���':'volumn',u'������������':'capital_inflow',u'������������':'capital_inflow_rate',
                          u'��������':'capital_compass',u'��������':'inflow',u'��������':'inflow_rate',
                          u'����':'TBD',u'��ҵ��������':'industry_inflow_rate',u'��ҵ��������':'industry_inflow',
                          u'����':'big_one',u'��С��':'small_one',u'��ɢ��':'bulk_single',
                          u'������':'super_one',u'��':'big_sum',u'С��':'small_list',
                          u'ɢ��':'bulk_sum',u'����':'min_one'},inplace=True)
    predit_value = LSTMModel(stock.close_price,3)

