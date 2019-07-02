import pandas as pd

data = pd.read_csv('train_new.csv', low_memory = False)
print(data.shape)

train = data[data['start_time'] < '2018-06-23 00:00:00']
test = data[data['start_time'] >= '2018-06-23 00:00:00' ]

print(train.shape, test.shape)

#train.to_csv('train_7_day.csv', index = False)
#test.to_csv('test_7_day.csv', index = False)