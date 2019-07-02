import pymysql
from math import exp
# 打开数据库连接
db = pymysql.connect("192.168.200.241", "root", "root", "mydb", charset='utf8' )

# 使用cursor()方法获取操作游标 
cursor = db.cursor()

for t in ['bayes']:
    for k in [10]:
        
        # 使用execute方法执行SQL语句
        if t is None:
            cursor.execute("SELECT * from u_trip_res where k=" + str(k) + " and type is null")
        else:
            cursor.execute("SELECT * from u_trip_res where k=" + str(k) + " and type='" + t + "'")

        # 使用 fetchone() 方法获取一条数据
        data = cursor.fetchall()

        total = 0
        lt_500_count = 0
        lt_100_count = 0
        lt_200_count = 0
        lt_1000_count = 0
        gt_1000_count = 0
        for item in data:
            total += 1 / (1 + exp((-item[5] + 1000) / 250))
            if(item[5] <= 1000):
                lt_1000_count += 1
                if(item[5] <= 500):
                    lt_500_count += 1
                    if(item[5] <= 200):
                        lt_200_count += 1
                        if(item[5] <= 1000):
                            lt_100_count += 1
            else:
                gt_1000_count += 1
        print('k: ' + str(k) + ', type: ' + (t if t is not None else 'null,') + ',score: ' + str(total / len(data)))
        print(lt_100_count, lt_200_count, lt_500_count, lt_1000_count, gt_1000_count)
# 关闭数据库连接
db.close()

