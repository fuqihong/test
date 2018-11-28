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



key_cal = 'third_count_2'
sc = SparkContext(appName=key_cal + "_sql_daily")
hsqlContext = HiveContext(sc)




kkOrgCountInitDf = hsqlContext.sql("select idcard, "
                                   "mec_type,"
                                   "datediff(current_date(),repay_tm) as day_pay,"
                                   "no_mec,"
                                   "pay_result,"
                                   "count(1) as times "
                                   "from {mid_table}  aa group by idcard,mec_type,datediff(current_date(),repay_tm),no_mec,pay_result".format(mid_table=input_mid_table_name))

hsqlContext.registerDataFrameAsTable(kkOrgCountInitDf, "kkOrgCountInit")

kkOrgCountMidDf = hsqlContext.sql("select distinct aa.idcard,"
                                  "aa.mec_type,"
                                  "case when day_pay <= 90 then 6 when day_pay <= 180 then 7 when day_pay <= 360 then 8 else 9 end as day_pay,"
                                  "aa.no_mec,"
                                  "aa.pay_result,"
                                  "case when times > 100 then 5 when times > 50 then 4 when times > 10 then 3 when times > 0 then 2 else 1 end as times from kkOrgCountInit aa")

hsqlContext.registerDataFrameAsTable(kkOrgCountMidDf, "kkOrgCountMid")

kkOrgCountDf = hsqlContext.sql("select idcard,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 6 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td041,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 6 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td042,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 6 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td043,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 6 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td044,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 6 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td045,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 6 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td046,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 6 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td047,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 6 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td048,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 7 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td049,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 7 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td050,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 7 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td051,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 7 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td052,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 7 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td053,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 7 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td054,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 7 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td055,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 7 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td056,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 8 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td057,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 8 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td058,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 8 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td059,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 8 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td060,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 8 and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td061,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 8 and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td062,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 8 and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td063,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 8 and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td064,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 6 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td075,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 6 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td076,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 7 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td077,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 7 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td078,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.day_pay <= 8 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td079,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.day_pay <= 8 and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td080,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td081,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td082,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td083,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td084,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.pay_result = 0 and aa.times >= 2 then aa.no_mec else null end) as t03td085,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.pay_result = 0 and aa.times >= 3 then aa.no_mec else null end) as t03td086,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.pay_result = 0 and aa.times >= 4 then aa.no_mec else null end) as t03td087,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.pay_result = 0 and aa.times >= 5 then aa.no_mec else null end) as t03td088,"
                               "count(distinct case when aa.mec_type = 'cf' and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td089,"
                               "count(distinct case when aa.mec_type = 'sl' and aa.pay_result = 1 and aa.times >= 2 then aa.no_mec else null end) as t03td090 "
                               "from kkOrgCountMid aa group by aa.idcard")

save_path = output_feature_hdfs_path + key_cal
keys = kkOrgCountDf.rdd.map(lambda row: dropFrame(row))
keys.repartition(500).saveAsTextFile(save_path)

sc.stop()
