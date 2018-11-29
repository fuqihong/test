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


key_cal = 'countDayPay_1'
print key_cal + "_sql_daily" + " run " + "*"*90

sc = SparkContext(appName= key_cal + "_sql_daily")


hsqlContext = HiveContext(sc)


midsqlDf = hsqlContext.sql("select idcard,"
                           "no_mec as mec_no,"
                           "mec_type as goods_if_subbizcatname,"
                           "case when flag_error = 1 then 1 when flag_error > 1 then 2 else 3 end as req_if_trademsg,"
                           "pay_result as pay_result,"
                           "datediff('{current_time}', repay_tm) as day_pay "
                           "from {mid_table} and repay_tm >= date_sub('{current_time}',21)".format(current_time = yes_time,mid_table=input_mid_table_name))

hsqlContext.registerDataFrameAsTable(midsqlDf, "personal_cfsl_loan_deduct_seq_mid")

countDayPayMidDf = hsqlContext.sql("select distinct idcard,"
                                   "mec_no,"
                                   "goods_if_subbizcatname,"
                                   "req_if_trademsg,"
                                   "pay_result,"
                                   "case when day_pay <= 1 then 1 when day_pay <= 7 then 2 when day_pay <= 14 then 3 when day_pay <= 21 then 4 when day_pay <= 30 then 5 when day_pay <= 90 then 6 when day_pay <= 180 then 7 when day_pay <= 360 then 8 else 9 end as day_pay "
                                   "from  personal_cfsl_loan_deduct_seq_mid")

hsqlContext.registerDataFrameAsTable(countDayPayMidDf, "personal_cfsl_loan_deduct_seq_countDayPay_mid")

countsqlDf = hsqlContext.sql("select idcard,"
                             "count(distinct case when day_pay<=1 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01deazaa,"
                             "count(distinct case when day_pay<=1 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01deazab,"
                             "count(distinct case when day_pay<=1 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01deazac,"
                             "count(distinct case when day_pay<=1 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01deazaz,"
                             "count(distinct case when day_pay<=1 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01deazba,"
                             "count(distinct case when day_pay<=1 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01deazbb,"
                             "count(distinct case when day_pay<=1 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01deazbc,"
                             "count(distinct case when day_pay<=1 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01deazbz,"
                             "count(distinct case when day_pay<=1 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01deazza,"
                             "count(distinct case when day_pay<=1 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01deazzb,"
                             "count(distinct case when day_pay<=1 and pay_result =1 then mec_no else null end) as t01deazzc,"
                             "count(distinct case when day_pay<=1 then mec_no else null end) as t01deazzz,"
                             "count(distinct case when day_pay<=2 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01debzaa,"
                             "count(distinct case when day_pay<=2 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01debzab,"
                             "count(distinct case when day_pay<=2 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01debzac,"
                             "count(distinct case when day_pay<=2 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01debzaz,"
                             "count(distinct case when day_pay<=2 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01debzba,"
                             "count(distinct case when day_pay<=2 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01debzbb,"
                             "count(distinct case when day_pay<=2 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01debzbc,"
                             "count(distinct case when day_pay<=2 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01debzbz,"
                             "count(distinct case when day_pay<=2 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01debzza,"
                             "count(distinct case when day_pay<=2 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01debzzb,"
                             "count(distinct case when day_pay<=2 and pay_result =1 then mec_no else null end) as t01debzzc,"
                             "count(distinct case when day_pay<=2 then mec_no else null end) as t01debzzz,"
                             "count(distinct case when day_pay<=3 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01deczaa,"
                             "count(distinct case when day_pay<=3 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01deczab,"
                             "count(distinct case when day_pay<=3 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01deczac,"
                             "count(distinct case when day_pay<=3 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01deczaz,"
                             "count(distinct case when day_pay<=3 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01deczba,"
                             "count(distinct case when day_pay<=3 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01deczbb,"
                             "count(distinct case when day_pay<=3 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01deczbc,"
                             "count(distinct case when day_pay<=3 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01deczbz,"
                             "count(distinct case when day_pay<=3 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01deczza,"
                             "count(distinct case when day_pay<=3 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01deczzb,"
                             "count(distinct case when day_pay<=3 and pay_result =1 then mec_no else null end) as t01deczzc,"
                             "count(distinct case when day_pay<=3 then mec_no else null end) as t01deczzz,"
                             "count(distinct case when day_pay<=4 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dedzaa,"
                             "count(distinct case when day_pay<=4 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dedzab,"
                             "count(distinct case when day_pay<=4 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01dedzac,"
                             "count(distinct case when day_pay<=4 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01dedzaz,"
                             "count(distinct case when day_pay<=4 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dedzba,"
                             "count(distinct case when day_pay<=4 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dedzbb,"
                             "count(distinct case when day_pay<=4 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01dedzbc,"
                             "count(distinct case when day_pay<=4 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01dedzbz,"
                             "count(distinct case when day_pay<=4 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dedzza,"
                             "count(distinct case when day_pay<=4 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dedzzb,"
                             "count(distinct case when day_pay<=4 and pay_result =1 then mec_no else null end) as t01dedzzc,"
                             "count(distinct case when day_pay<=4 then mec_no else null end) as t01dedzzz "
                             "from personal_cfsl_loan_deduct_seq_countDayPay_mid yy group by yy.idcard")

save_path = output_feature_hdfs_path + key_cal
keys = countsqlDf.rdd.map(lambda row: dropFrame(row))
keys.repartition(500).saveAsTextFile(save_path)
#keys.repartition(500).saveAsTextFile('/Users/xilin.zheng/yeepay/PRD4/data3/countDayPay_1')
sc.stop()
print key_cal + "_sql_daily" + " success " + "*"*90