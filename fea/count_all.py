#!/usr/bin/python
# encoding:utf-8

# @author:xilin.zheng

# @file:mid.py.py

# @time:2018/11/16 下午4:45
from config import output_feature_hdfs_path,input_mid_table_name,dropFrame
from pyspark import SparkContext
from pyspark.sql import HiveContext
import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8')

key_cal = 'countAll'
print key_cal + "_sql_daily" + " run " + "*"*90


sc = SparkContext(appName = key_cal + "_sql_daily")
hsqlContext = HiveContext(sc)

midsqlDf = hsqlContext.sql("select distinct idcard,"
                           "no_mec as mec_no,"
                           "mec_type as goods_if_subbizcatname,"
                           "case when flag_error = 1 then 1 when flag_error > 1 then 2 else 3 end as req_if_trademsg,"
                           "pay_result as pay_result "
                           "from {mid_table}".format(mid_table=input_mid_table_name))

hsqlContext.registerDataFrameAsTable(midsqlDf, "personal_cfsl_loan_deduct_seq_mid")

countsqlDf = hsqlContext.sql("select idcard,"
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
                             "from personal_cfsl_loan_deduct_seq_mid yy group by yy.idcard")

keys = countsqlDf.rdd.map(lambda row: dropFrame(row))
save_path = output_feature_hdfs_path + key_cal
keys.repartition(500).saveAsTextFile(save_path)


sc.stop()


print key_cal + "_sql_daily" + " success " + "*"*90