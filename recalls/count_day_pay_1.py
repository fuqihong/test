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

key_cal = 'countDayPay_1'
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



midsqlDf = hsqlContext.sql("select  aa.idcard,aa.recall_date,"
                           "no_mec as mec_no,"
                           "mec_type as goods_if_subbizcatname,"
                           "case when flag_error = 1 then 1 when flag_error > 1 then 2 else 3 end as req_if_trademsg,"
                           "pay_result as pay_result,"
                           "datediff(aa.recall_date, repay_tm) as day_pay "
                           "from personal_cfsl_loan_deduct_seq aa "
                           " where repay_tm >= date_sub(aa.recall_date,21)")

hsqlContext.registerDataFrameAsTable(midsqlDf, "personal_cfsl_loan_deduct_seq_mid")

countDayPayMidDf = hsqlContext.sql("select distinct idcard,recall_date,"
                                   "mec_no,"
                                   "goods_if_subbizcatname,"
                                   "req_if_trademsg,"
                                   "pay_result,"
                                   "case when day_pay <= 1 then 1 when day_pay <= 7 then 2 when day_pay <= 14 then 3 when day_pay <= 21 then 4 when day_pay <= 30 then 5 when day_pay <= 90 then 6 when day_pay <= 180 then 7 when day_pay <= 360 then 8 else 9 end as day_pay "
                                   "from  personal_cfsl_loan_deduct_seq_mid")

hsqlContext.registerDataFrameAsTable(countDayPayMidDf, "personal_cfsl_loan_deduct_seq_countDayPay_mid")

countsqlDf = hsqlContext.sql("select idcard,recall_date,"
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
                             "from personal_cfsl_loan_deduct_seq_countDayPay_mid yy group by yy.idcard,yy.recall_date")

keys = countsqlDf.rdd.map(lambda row: dropFrame(row))
keys.repartition(100).saveAsTextFile(output_feature_path_pre + output_path + '/' + key_cal)
#keys.repartition(500).saveAsTextFile('/Users/xilin.zheng/yeepay/PRD4/data4/countDayPay_1')
sc.stop()

print key_cal + "_sql_daily" + " success " + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))  +  "*"*90
