import datetime
import os
import time
from collections import Counter

import geohash
import pandas as pd
from sklearn.neighbors import KNeighborsClassifier
from sklearn.utils import shuffle

def datetime_to_period(date_str):
    """
    描述：把时间分为24段
    返回：0到23
    """
    time_part = date_str.split(" ")[1]  # 获取时间部分
    hour_part = int(time_part.split(":")[0])  # 获取小时
    return hour_part

def date_to_period(date_str):
    """
    描述：把日期转化为对应的工作日或者节假日
    返回：0:工作日 1：节假日 2:小长假
    """
    holiday_list = ['2018-01-01', '2018-02-15', '2018-02-16', '2018-02-17', '2018-02-18', '2018-02-19',
                    '2018-02-20', '2018-02-21', '2018-04-05', '2018-04-06', '2018-04-07', '2018-04-29',
                    '2018-04-30', '2018-05-01', '2018-06-16', '2018-06-17', '2018-06-18']  # 小长假
    switch_workday_list = ['2018-02-11', '2018-02-24', '2018-04-08', '2018-04-28']  # 小长假补班
    workday_list = ['1', '2', '3', '4', '5']  # 周一到周五
    weekday_list = ['0', '6']  # 周六、周日，其中0表示周日
    date = date_str.split(" ")[0]  # 获取日期部分
    whatday = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').strftime("%w")  # 把日期转化为星期
    if date in holiday_list:
        return 2
    elif date in switch_workday_list:
        return 0
    elif whatday in workday_list:
        return 0
    elif whatday in weekday_list:
        return 1


time_start = time.asctime(time.localtime(time.time()))  # 程序开始时间


train_data_path = "train_7_day.csv"
train_data = pd.read_csv(train_data_path, low_memory=False)
test_data_path = "test_7_day.csv"
test_data = pd.read_csv(test_data_path, low_memory=False)

n = 0
test_out_id = Counter(test_data['out_id'])
for k in [5, 10, 20, 30]:
    for out_id in test_out_id.keys():
        # ----train_new数据补充字段 begin----
        train = train_data[train_data['out_id'] == out_id]  # 选择出同一个out_id的数据
        train = shuffle(train)  # 打乱顺序
        train['start_code'] = None  # 开始位置的geohash编码
        train['end_code'] = None  # 结束位置的geohash编码
        train['period'] = None  # 时间段编码（0-23）
        train['week_code'] = None  # 工作日和休息日编码
        for i in range(len(train)):
            train.iloc[i, 8] = geohash.encode(train.iloc[i, 4], train.iloc[i, 5], 8)  # 开始geohash编码
            train.iloc[i, 9] = geohash.encode(train.iloc[i, 6], train.iloc[i, 7], 8)  # 结束geohash编码
            train.iloc[i, 10] = datetime_to_period(train.iloc[i, 2])  # 添加时间段
            train.iloc[i, 11] = date_to_period(train.iloc[i, 2])  # 添加工作日、休息日编码
        # ----train_new数据补充字段 end----

        # ----test_new数据补充字段 begin----
        test = test_data[test_data['out_id'] == out_id]
        test = shuffle(test)  # 打乱顺序
        test['period'] = None
        test['week_code'] = None
        test['start_code'] = None
        test['predict'] = None
        for i in range(len(test)):
            test.iloc[i, 7] = datetime_to_period(test.iloc[i, 2])  # 添加时间段
            test.iloc[i, 8] = date_to_period(test.iloc[i, 2])  # 添加工作日、休息日编码
            test.iloc[i, 9] = geohash.encode(test.iloc[i, 3], test.iloc[i, 4], 8)  # 开始geohash编码
        # ----test_new数据补充字段 end----

        # ---knn begin---
        knn = KNeighborsClassifier(n_neighbors=10, weights='distance', algorithm='auto', p=2)
        knn.fit(train[['start_lat', 'start_lon', 'period', 'week_code']], train['end_code'])
        predict = knn.predict(test[['start_lat', 'start_lon', 'period', 'week_code']])
        test['predict'] = predict
        # ---knn end---
        if n == 0:
            test.to_csv("predict" + str(k) + "_7_day.csv", mode='a', encoding='utf-8', index=False,
                        header=True)
        else:
            test.to_csv("predict" + str(k) + "_7_day.csv", mode='a', encoding='utf-8', index=False,
                        header=False)
        if n % 500 == 0:
            print("已运行：" + str(n) + " " + time.asctime(time.localtime(time.time())))
        n = n + 1
    print("输出结果：\n")
    df = pd.read_csv("predict" + str(k) + "_7_day.csv")  # 预测结果文件
    df['end_lat_pre'] = None
    df['end_lon_pre'] = None
    for i in range(len(df)):
        site = geohash.decode(df.iloc[i, 10])
        df.iloc[i, 11] = site[0]  # 预测横坐标
        df.iloc[i, 12] = site[1]  # 预测纵坐标
        if i % 5000 == 0:
            print("已运行" + str(i))
    df = df[['r_key', 'end_lat', 'end_lon', 'end_lat_pre', 'end_lon_pre']]
    df.to_csv("predict_result" + str(k) + "_7_day.csv", encoding='utf-8', index=False)

    print('\r程序运行开始时间：', time_start)
    print('\r程序运行结束时间：', time.asctime(time.localtime(time.time())))
