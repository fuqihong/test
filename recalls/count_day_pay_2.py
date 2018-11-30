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

key_cal = 'countDayPay_2'
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


midsqlDf = hsqlContext.sql("select aa.idcard,aa.recall_date,"
                           "no_mec as mec_no,"
                           "mec_type as goods_if_subbizcatname,"
                           "case when flag_error = 1 then 1 when flag_error > 1 then 2 else 3 end as req_if_trademsg,"
                           "pay_result as pay_result,"
                           "datediff(aa.recall_date, repay_tm) as day_pay "
                           "from personal_cfsl_loan_deduct_seq aa "
                           "where repay_tm >= date_sub(aa.recall_date,360)")

hsqlContext.registerDataFrameAsTable(midsqlDf, "personal_cfsl_loan_deduct_seq_mid")

countDayPayMidDf = hsqlContext.sql("select distinct idcard,recall_date,"
                                   "mec_no,"
                                   "goods_if_subbizcatname,"
                                   "req_if_trademsg,"
                                   "pay_result,"
                                   "case when day_pay <= 30 then 5 when day_pay <= 90 then 6 when day_pay <= 180 then 7 when day_pay <= 360 then 8 else 9 end as day_pay "
                                   "from  personal_cfsl_loan_deduct_seq_mid")

hsqlContext.registerDataFrameAsTable(countDayPayMidDf, "personal_cfsl_loan_deduct_seq_countDayPay_mid")

countsqlDf = hsqlContext.sql("select idcard,recall_date,"
                             "count(distinct case when day_pay<=5 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01deezaa,"
                             "count(distinct case when day_pay<=5 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01deezab,"
                             "count(distinct case when day_pay<=5 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01deezac,"
                             "count(distinct case when day_pay<=5 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01deezaz,"
                             "count(distinct case when day_pay<=5 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01deezba,"
                             "count(distinct case when day_pay<=5 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01deezbb,"
                             "count(distinct case when day_pay<=5 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01deezbc,"
                             "count(distinct case when day_pay<=5 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01deezbz,"
                             "count(distinct case when day_pay<=5 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01deezza,"
                             "count(distinct case when day_pay<=5 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01deezzb,"
                             "count(distinct case when day_pay<=5 and pay_result =1 then mec_no else null end) as t01deezzc,"
                             "count(distinct case when day_pay<=5 then mec_no else null end) as t01deezzz,"
                             "count(distinct case when day_pay<=6 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01defzaa,"
                             "count(distinct case when day_pay<=6 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01defzab,"
                             "count(distinct case when day_pay<=6 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01defzac,"
                             "count(distinct case when day_pay<=6 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01defzaz,"
                             "count(distinct case when day_pay<=6 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01defzba,"
                             "count(distinct case when day_pay<=6 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01defzbb,"
                             "count(distinct case when day_pay<=6 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01defzbc,"
                             "count(distinct case when day_pay<=6 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01defzbz,"
                             "count(distinct case when day_pay<=6 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01defzza,"
                             "count(distinct case when day_pay<=6 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01defzzb,"
                             "count(distinct case when day_pay<=6 and pay_result =1 then mec_no else null end) as t01defzzc,"
                             "count(distinct case when day_pay<=6 then mec_no else null end) as t01defzzz,"
                             "count(distinct case when day_pay<=7 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01degzaa,"
                             "count(distinct case when day_pay<=7 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01degzab,"
                             "count(distinct case when day_pay<=7 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01degzac,"
                             "count(distinct case when day_pay<=7 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01degzaz,"
                             "count(distinct case when day_pay<=7 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01degzba,"
                             "count(distinct case when day_pay<=7 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01degzbb,"
                             "count(distinct case when day_pay<=7 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01degzbc,"
                             "count(distinct case when day_pay<=7 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01degzbz,"
                             "count(distinct case when day_pay<=7 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01degzza,"
                             "count(distinct case when day_pay<=7 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01degzzb,"
                             "count(distinct case when day_pay<=7 and pay_result =1 then mec_no else null end) as t01degzzc,"
                             "count(distinct case when day_pay<=7 then mec_no else null end) as t01degzzz,"
                             "count(distinct case when day_pay<=8 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dehzaa,"
                             "count(distinct case when day_pay<=8 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dehzab,"
                             "count(distinct case when day_pay<=8 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01dehzac,"
                             "count(distinct case when day_pay<=8 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01dehzaz,"
                             "count(distinct case when day_pay<=8 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dehzba,"
                             "count(distinct case when day_pay<=8 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dehzbb,"
                             "count(distinct case when day_pay<=8 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01dehzbc,"
                             "count(distinct case when day_pay<=8 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01dehzbz,"
                             "count(distinct case when day_pay<=8 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dehzza,"
                             "count(distinct case when day_pay<=8 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dehzzb,"
                             "count(distinct case when day_pay<=8 and pay_result =1 then mec_no else null end) as t01dehzzc,"
                             "count(distinct case when day_pay<=8 then mec_no else null end) as t01dehzzz "
                             "from personal_cfsl_loan_deduct_seq_countDayPay_mid yy group by yy.idcard,yy.recall_date")

keys = countsqlDf.rdd.map(lambda row: dropFrame(row))
keys.repartition(100).saveAsTextFile(output_feature_path_pre + output_path + '/' + key_cal)
sc.stop()

print key_cal + "_sql_daily" + " success " + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))  +  "*"*90
