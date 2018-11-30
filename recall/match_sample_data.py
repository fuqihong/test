#!/usr/bin/python
# encoding:utf-8

# @author:qihong.fu
import pandas 
from config import input_sample_path_pre,output_sample_data_path_pre,dropFrame
from pyspark import SparkContext
from pyspark.sql import HiveContext
import sys
import re
import time

reload(sys)
sys.setdefaultencoding('utf-8')


key_cal = 'match_sample_data'
print key_cal + "_sql_daily" + " run " + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))  +  "*"*90



sc = SparkContext(appName= key_cal + "_sql_daily")
hsqlContext = HiveContext(sc)

input_path = sys.argv[1]
recallPath = input_sample_path_pre + input_path
recallRDDS= sc.textFile(recallPath)
recallRDD = recallRDDS.map(lambda x: x.split(',')).map(lambda row: (row[0], row[1]))
recallDf = hsqlContext.createDataFrame(recallRDD,
                                         ['idcard', 'recall_date'])
hsqlContext.registerDataFrameAsTable(recallDf, "t_recall")


df_count = hsqlContext.sql("select max(recall_date) as max_dt, count(1)  as all_samples_count from t_recall")
df_count = df_count.toPandas()
max_dt = str(df_count['max_dt'].values[0])

sql_sent = """
select a.id_pay, a.idcard, a.no_card, a.no_mec, a.mec_type, a.repay_tm, a.pay_result, a.amt, a.flag_error, 
      a.month, a.day, a.amt_s, b.recall_date
      from dwd.fea_personal_cfsl_loan_deduct_seq_daily a
      inner join t_recall b
      on a.idcard = b.idcard
      where a.dt <= '{max_dt}'
      and b.recall_date > a.repay_tm
      and b.idcard is not null
""".format(max_dt = max_dt)

matchDf = hsqlContext.sql(sql_sent)

hsqlContext.registerDataFrameAsTable(matchDf, "df_match")
df_match_count = hsqlContext.sql("select count(distinct idcard) as match_count from df_match")
df_match_count = df_match_count.toPandas()


output_path =  input_path.replace('.csv', '').replace('/*', '')

keySeconds = matchDf.rdd.map(lambda row: dropFrame(row))
keySeconds.repartition(10).saveAsTextFile(output_sample_data_path_pre + output_path)

print "samplesize: " + str(df_count.all_samples_count.values[0])
print "matchsamplesize: " + str(df_match_count.match_count.values[0])
sc.stop()

print key_cal + "_sql_daily" + " success " + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))  +  "*"*90