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



key_cal = 'third_count_1'
sc = SparkContext(appName=key_cal + "_sql_daily")
hsqlContext = HiveContext(sc)



kkOrgCountInitDf = hsqlContext.sql("select idcard, "
                                   "mec_type,"
                                   "datediff(current_date(),repay_tm) as day_pay,"
                                   "no_mec,"
                                   "pay_result,"
                                   "count(1) as times "
                                   "from {mid_table} group by idcard,mec_type,datediff(current_date(),repay_tm),no_mec,pay_result".format(mid_table=input_mid_table_name))

hsqlContext.registerDataFrameAsTable(kkOrgCountInitDf, "kkOrgCountInit")

kkOrgCountMidDf = hsqlContext.sql("select distinct aa.idcard,"
                                  "aa.mec_type,"
                                  "case when day_pay <= 1 then 1 when day_pay <= 7 then 2 when day_pay <= 14 then 3 when day_pay <= 21 then 4 when day_pay <= 30 then 5 when day_pay <= 90 then 6 when day_pay <= 180 then 7 when day_pay <= 360 then 8 else 9 end as day_pay,"
                                  "aa.no_mec,"
                                  "aa.pay_result,"
                                  "case when times > 100 then 5 when times > 50 then 4 when times > 10 then 3 when times > 0 then 2 else 1 end as times from kkOrgCountInit aa")

hsqlContext.registerDataFrameAsTable(kkOrgCountMidDf, "kkOrgCountMid")

kkOrgCountDf = hsqlContext.sql("select aa.idcard,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 1 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td001,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 1 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td002,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 1 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td003,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 1 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td004,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 1 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td005,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 1 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td006,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 1 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td007,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 1 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td008,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 2 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td009,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 2 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td010,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 2 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td011,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 2 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td012,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 2 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td013,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 2 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td014,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 2 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td015,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 2 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td016,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 3 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td017,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 3 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td018,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 3 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td019,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 3 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td020,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 3 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td021,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 3 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td022,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 3 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td023,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 3 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td024,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 4 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td025,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 4 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td026,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 4 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td027,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 4 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td028,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 4 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td029,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 4 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td030,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 4 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td031,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 4 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td032,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 5 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td033,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 5 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td034,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 5 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td035,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 5 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td036,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 5 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td037,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 5 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td038,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 5 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td039,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 5 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td040,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 1 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td065,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 1 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td066,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 2 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td067,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 2 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td068,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 3 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td069,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 3 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td070,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 4 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td071,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 4 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td072,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 5 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td073,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 5 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td074 "
                               "from kkOrgCountMid aa group by aa.idcard")



save_path = output_feature_hdfs_path + key_cal
keys = kkOrgCountDf.rdd.map(lambda row: dropFrame(row))
keys.repartition(500).saveAsTextFile(save_path)
sc.stop()
