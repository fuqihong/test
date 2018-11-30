#!/usr/bin/python
# encoding:utf-8

# @author:xilin.zheng

# @file:mid.py.py

# @time:2018/11/16 下午4:45
from config import output_sample_data_path_pre,output_feature_path_pre,dropFrame
from pyspark import SparkContext
from pyspark.sql import HiveContext
import sys
import re
import time

reload(sys)
sys.setdefaultencoding('utf-8')

key_cal = 'countAll'
print key_cal + "_sql_daily" + " run " + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))  +  "*"*90



sc = SparkContext(appName= key_cal + "_sql_daily")
hsqlContext = HiveContext(sc)

input_path = sys.argv[1]
output_path =  input_path.replace('.csv', '').replace('/*', '')

match_sample_data_path = output_sample_data_path_pre + output_path


matchRDDS = sc.textFile(match_sample_data_path)

matchRDD = matchRDDS.map(lambda x: x.split(',')).map(lambda row: (
    row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12]))
midDf = hsqlContext.createDataFrame(matchRDD,
                                    ['id_pay', 'idcard', 'no_card', 'no_mec', 'mec_type', 'repay_tm', 'pay_result',
                                     'amt', 'flag_error', 'month', 'day', 'amt_s', 'recall_date'])

hsqlContext.registerDataFrameAsTable(midDf, "personal_cfsl_loan_deduct_seq")



midsqlDf = hsqlContext.sql("select distinct aa.idcard,aa.recall_date,"
                           "no_mec as mec_no,"
                           "mec_type as goods_if_subbizcatname,"
                           "case when flag_error = 1 then 1 when flag_error > 1 then 2 else 3 end as req_if_trademsg,"
                           "pay_result as pay_result "
                           "from personal_cfsl_loan_deduct_seq aa ")

hsqlContext.registerDataFrameAsTable(midsqlDf, "personal_cfsl_loan_deduct_seq_mid")

countsqlDf = hsqlContext.sql("select idcard,recall_date,"
                             "sum(case when goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dezzaa,"
                             "sum(case when goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then 1 else 0 end) as t01dezzab,"
                             "count(distinct case when goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01dezzac,"
                             "count(distinct case when goods_if_subbizcatname = 'cf' then mec_no else null end) as t01dezzaz,"
                             "sum(case when goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dezzba,"
                             "sum(case when goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then 1 else 0 end) as t01dezzbb,"
                             "count(distinct case when goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01dezzbc,"
                             "count(distinct case when goods_if_subbizcatname ='sl' then mec_no else null end) as t01dezzbz,"
                             "count(distinct case when pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezzza,"
                             "count(distinct case when pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezzzb,"
                             "count(distinct case when pay_result =1 then mec_no else null end) as t01dezzzc,"
                             "count(distinct mec_no) as t01dezzzz "
                             "from personal_cfsl_loan_deduct_seq_mid yy group by yy.idcard,yy.recall_date")

keys = countsqlDf.rdd.map(lambda row: dropFrame(row))
keys.repartition(10).saveAsTextFile(output_feature_path_pre + output_path + '/' + key_cal)

sc.stop()

print key_cal + "_sql_daily" + " success " + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))  +  "*"*90



