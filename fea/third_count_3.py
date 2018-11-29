#!/usr/bin/python
# encoding:utf-8

# @author:xilin.zheng

# @file:mid.py.py

# @time:2018/11/16 下午4:45
from config import output_feature_hdfs_path,input_mid_table_name,dropFrame,yes_time
from pyspark import SparkContext
from pyspark.sql import HiveContext
import sys
import re

reload(sys)
sys.setdefaultencoding('utf-8')


key_cal = 'third_count_3'
print key_cal + "_sql_daily" + " run " + "*"*90
sc = SparkContext(appName=key_cal + "_sql_daily")

hsqlContext = HiveContext(sc)



kkOrgCountInitDf = hsqlContext.sql("select idcard, "
                                   "mec_type,"
                                   "datediff('{current_time}',repay_tm) as day_pay,"
                                   "no_mec,"
                                   "pay_result,"
                                   "count(1) as times "
                                   "from {mid_table} group by idcard,mec_type,datediff('{current_time}', repay_tm),no_mec,pay_result".format(current_time=yes_time,mid_table=input_mid_table_name))

hsqlContext.registerDataFrameAsTable(kkOrgCountInitDf, "kkOrgCountInit")

kkOrgCountMidDf = hsqlContext.sql("select distinct aa.idcard,"
                                  "case when day_pay <= 1 then 1 when day_pay <= 7 then 2 when day_pay <= 14 then 3 when day_pay <= 21 then 4 when day_pay <= 30 then 5 when day_pay <= 90 then 6 when day_pay <= 180 then 7 when day_pay <= 360 then 8 else 9 end as day_pay,"
                                  "aa.no_mec,"
                                  "aa.pay_result,"
                                  "case when times > 100 then 5 when times > 50 then 4 when times > 10 then 3 when times > 0 then 2 else 1 end as times from kkOrgCountInit aa")

hsqlContext.registerDataFrameAsTable(kkOrgCountMidDf, "kkOrgCountMid")

kkOrgCountDf = hsqlContext.sql("select idcard,"
                               "count(distinct case when aa.day_pay <= 1 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td091,"
                               "count(distinct case when aa.day_pay <= 1 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td092,"
                               "count(distinct case when aa.day_pay <= 1 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td093,"
                               "count(distinct case when aa.day_pay <= 1 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td094,"
                               "count(distinct case when aa.day_pay <= 2 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td095,"
                               "count(distinct case when aa.day_pay <= 2 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td096,"
                               "count(distinct case when aa.day_pay <= 2 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td097,"
                               "count(distinct case when aa.day_pay <= 2 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td098,"
                               "count(distinct case when aa.day_pay <= 3 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td099,"
                               "count(distinct case when aa.day_pay <= 3 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td100,"
                               "count(distinct case when aa.day_pay <= 3 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td101,"
                               "count(distinct case when aa.day_pay <= 3 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td102,"
                               "count(distinct case when aa.day_pay <= 4 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td103,"
                               "count(distinct case when aa.day_pay <= 4 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td104,"
                               "count(distinct case when aa.day_pay <= 4 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td105,"
                               "count(distinct case when aa.day_pay <= 4 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td106,"
                               "count(distinct case when aa.day_pay <= 5 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td107,"
                               "count(distinct case when aa.day_pay <= 5 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td108,"
                               "count(distinct case when aa.day_pay <= 5 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td109,"
                               "count(distinct case when aa.day_pay <= 5 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td110,"
                               "count(distinct case when aa.day_pay <= 6 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td111,"
                               "count(distinct case when aa.day_pay <= 6 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td112,"
                               "count(distinct case when aa.day_pay <= 6 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td113,"
                               "count(distinct case when aa.day_pay <= 6 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td114,"
                               "count(distinct case when aa.day_pay <= 7 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td115,"
                               "count(distinct case when aa.day_pay <= 7 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td116,"
                               "count(distinct case when aa.day_pay <= 7 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td117,"
                               "count(distinct case when aa.day_pay <= 7 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td118,"
                               "count(distinct case when aa.day_pay <= 8 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td119,"
                               "count(distinct case when aa.day_pay <= 8 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td120,"
                               "count(distinct case when aa.day_pay <= 8 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td121,"
                               "count(distinct case when aa.day_pay <= 8 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td122,"
                               "count(distinct case when aa.day_pay <= 1 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td123,"
                               "count(distinct case when aa.day_pay <= 2 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td124,"
                               "count(distinct case when aa.day_pay <= 3 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td125,"
                               "count(distinct case when aa.day_pay <= 4 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td126,"
                               "count(distinct case when aa.day_pay <= 5 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td127,"
                               "count(distinct case when aa.day_pay <= 6 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td128,"
                               "count(distinct case when aa.day_pay <= 7 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td129,"
                               "count(distinct case when aa.day_pay <= 8 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td130,"
                               "count(distinct case when aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td131,"
                               "count(distinct case when aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td132,"
                               "count(distinct case when aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td133,"
                               "count(distinct case when aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td134,"
                               "count(distinct case when aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td135 "
                               "from kkOrgCountMid aa group by aa.idcard")

save_path = output_feature_hdfs_path + key_cal
keys = kkOrgCountDf.rdd.map(lambda row: dropFrame(row))
keys.repartition(500).saveAsTextFile(save_path)

sc.stop()
print key_cal + "_sql_daily" + " success " + "*"*90