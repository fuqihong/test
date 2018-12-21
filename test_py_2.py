
# coding: utf-8

# In[1]:


# coding: utf-8
import numpy as np
import pandas as pd


import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
import tensorflow as tf
from tensorflow.contrib.timeseries.python.timeseries import  NumpyReader


# In[3]:


from tensorflow.contrib.timeseries.python.timeseries import estimators as ts_estimators
from tensorflow.contrib.timeseries.python.timeseries import model as ts_model
from tensorflow.contrib.timeseries.python.timeseries import  NumpyReader


# In[4]:


df_result = pd.read_csv("df_result_4_3.csv")
column_name_list = list(df_result.columns)

dict_result = {}
x = np.array(range(23))


def main(data):
    reader = NumpyReader(data)

    train_input_fn = tf.contrib.timeseries.RandomWindowInputFn(
        reader, batch_size=5, window_size=10)

    ar = tf.contrib.timeseries.ARRegressor(
        periodicities=12, input_window_size=9, output_window_size=1,
        num_features=1,
        loss=tf.contrib.timeseries.ARModel.NORMAL_LIKELIHOOD_LOSS)

    ar.train(input_fn=train_input_fn, steps=1000)

    evaluation_input_fn = tf.contrib.timeseries.WholeDatasetInputFn(reader)
    # keys of evaluation: ['covariance', 'loss', 'mean', 'observed', 'start_tuple', 'times', 'global_step']
    evaluation = ar.evaluate(input_fn=evaluation_input_fn, steps=1)

    (predictions,) = tuple(ar.predict(
        input_fn=tf.contrib.timeseries.predict_continuation_input_fn(
            evaluation, steps=7)))
    return predictions['mean']



for i in column_name_list:
    print('*'*20 + str(i))
    y = np.array(df_result[i])
    data = {
        tf.contrib.timeseries.TrainEvalFeatures.TIMES: x,
        tf.contrib.timeseries.TrainEvalFeatures.VALUES: y,
    }
    list_y = list(y)
    try:
        predictions_mean = main(data)
        list_y.extend(list(predictions_mean.ravel()))
        dict_result[i] = list_y
    except:
        list_result = [np.mean(y)]*7
        list_y.extend(list_result)
        dict_result[i] = list_y
df_predict = pd.DataFrame.from_dict(dict_result)
df_predict.to_csv("df_predict_3.csv")




