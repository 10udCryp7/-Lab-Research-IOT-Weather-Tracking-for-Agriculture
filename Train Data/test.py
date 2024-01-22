import csv
import numpy as np
import numpy as np
import tensorflow as tf
from keras.models import Sequential
from keras.layers import Dense
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
from autils import *

import logging
logging.getLogger("tensorflow").setLevel(logging.ERROR)
tf.autograph.set_verbosity(0)

x = np.zeros((601, 11))
y = np.zeros((601, 1))

x_test = np.zeros((168, 11))
y_test = np.zeros((168, 1))
scaler_train_x = MinMaxScaler()
scaler_test_x = MinMaxScaler()
scaler_train_y = MinMaxScaler()
scaler_test_y = MinMaxScaler()

with open('data.csv', 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    list = list(csv_reader)
    data = np.array(list)
    # data_normalize = scaler.fit_transform(data[1:])
    # data_normalize = data[1:]
    count = 0
    for row in data[1:]:
        if (count <= 600):
            x[count] = [row[0], row[2], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[17]]
            y[count] = [row[3]]
        else:
            x_test[count - 600] = [row[0], row[2], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[17]]
            y_test[count - 600] = [row[3]]
        count += 1
    # scaler_train_x.fit_transform(x[0:])
    # scaler_train_y.fit_transform(y[0:])
    # scaler_test_x.fit_transform(x_test[0:])
    # scaler_test_y.fit_transform(y_test[0:])
print(np.shape(x))
print(np.shape(y))

model = Sequential(
    [
        tf.keras.Input(shape=(11,)),
        # tf.keras.layers.Dense(units = 500, activation = "linear"),
        # tf.keras.layers.Dense(units = 200, activation = "linear"),
        # tf.keras.layers.Dense(units = 100, activation = "linear"),
        # tf.keras.layers.Dense(units = 50, activation = "linear"),
        tf.keras.layers.Dense(units = 25, activation = "relu"),
        tf.keras.layers.Dense(units = 15, activation = "relu"),
        tf.keras.layers.Dense(units = 5, activation = "relu"),
        tf.keras.layers.Dense(units = 1, activation = "linear")
        ### END CODE HERE ### 
    ], name = "my_model" 
) 

model.compile(
    loss=tf.keras.losses.MeanSquaredError(),
    optimizer=tf.keras.optimizers.Adam(learning_rate = 0.001),
)

model.fit(
    x,y,
    epochs=500,   
)

# print(x_test)
y_predict = model.predict(x_test)
# y_predict.resize((18,68))
# y_test.resize((18,68))
# y_predict_trans = np.transpose(y_predict)
# y_test_trans = np.transpose(y_test)
# scaler.fit([data_normalize[1:,3]])
# yt_predict = scaler.inverse_transform(y_predict)
# yt_test = scaler.inverse_transform(y_test)

# yt_predict = scaler_test_y.inverse_transform(y_predict)
# yt_test = scaler_test_y.inverse_transform(y_test)
yt_predict = y_predict
yt_test = y_test
# print(y_predict)
for i in range(68):
    print(f"predict: {yt_predict[i, 0]}, test: {yt_test[i, 0]}")
print(np.corrcoef(yt_predict[:,0],yt_test[:, 0]))
print(mean_squared_error(yt_test[:, 0], yt_predict[:, 0]))
plt.scatter (yt_predict[:,0],yt_test[:,0])
plt.show()