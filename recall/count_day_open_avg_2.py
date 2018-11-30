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

key_cal = 'countDayOpen_2'
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
                           "datediff(aa.recall_date, first_value(repay_tm) over(partition by no_mec,aa.idcard,aa.recall_date order by repay_tm)) as day_open "
                           "from personal_cfsl_loan_deduct_seq aa ")

hsqlContext.registerDataFrameAsTable(midsqlDf, "personal_cfsl_loan_deduct_seq_mid")

countDayPayMidDf = hsqlContext.sql("select distinct idcard,recall_date,"
                                   "mec_no,"
                                   "goods_if_subbizcatname,"
                                   "req_if_trademsg,"
                                   "pay_result,"
                                   "case when day_open <= 30 then 5 when day_open <= 90 then 6 when day_open <= 180 then 7 when day_open <= 360 then 8 else 9 end as day_open "
                                   "from personal_cfsl_loan_deduct_seq_mid where day_open <= 360 ")

hsqlContext.registerDataFrameAsTable(countDayPayMidDf, "personal_cfsl_loan_deduct_seq_countDayOpen_mid")

countsqlDf = hsqlContext.sql("select idcard,recall_date,"
                             "count(distinct case when day_open<=5 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezeaa,"
                             "count(distinct case when day_open<=5 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezeab,"
                             "count(distinct case when day_open<=5 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01dezeac,"
                             "count(distinct case when day_open<=5 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01dezeaz,"
                             "count(distinct case when day_open<=5 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezeba,"
                             "count(distinct case when day_open<=5 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezebb,"
                             "count(distinct case when day_open<=5 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01dezebc,"
                             "count(distinct case when day_open<=5 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01dezebz,"
                             "count(distinct case when day_open<=5 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezeza,"
                             "count(distinct case when day_open<=5 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezezb,"
                             "count(distinct case when day_open<=5 and pay_result =1 then mec_no else null end) as t01dezezc,"
                             "count(distinct case when day_open<=5 then mec_no else null end) as t01dezezz,"
                             "count(distinct case when day_open<=6 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezfaa,"
                             "count(distinct case when day_open<=6 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezfab,"
                             "count(distinct case when day_open<=6 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01dezfac,"
                             "count(distinct case when day_open<=6 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01dezfaz,"
                             "count(distinct case when day_open<=6 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezfba,"
                             "count(distinct case when day_open<=6 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezfbb,"
                             "count(distinct case when day_open<=6 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01dezfbc,"
                             "count(distinct case when day_open<=6 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01dezfbz,"
                             "count(distinct case when day_open<=6 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezfza,"
                             "count(distinct case when day_open<=6 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezfzb,"
                             "count(distinct case when day_open<=6 and pay_result =1 then mec_no else null end) as t01dezfzc,"
                             "count(distinct case when day_open<=6 then mec_no else null end) as t01dezfzz,"
                             "count(distinct case when day_open<=7 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezgaa,"
                             "count(distinct case when day_open<=7 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezgab,"
                             "count(distinct case when day_open<=7 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01dezgac,"
                             "count(distinct case when day_open<=7 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01dezgaz,"
                             "count(distinct case when day_open<=7 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezgba,"
                             "count(distinct case when day_open<=7 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezgbb,"
                             "count(distinct case when day_open<=7 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01dezgbc,"
                             "count(distinct case when day_open<=7 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01dezgbz,"
                             "count(distinct case when day_open<=7 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezgza,"
                             "count(distinct case when day_open<=7 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezgzb,"
                             "count(distinct case when day_open<=7 and pay_result =1 then mec_no else null end) as t01dezgzc,"
                             "count(distinct case when day_open<=7 then mec_no else null end) as t01dezgzz,"
                             "count(distinct case when day_open<=8 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezhaa,"
                             "count(distinct case when day_open<=8 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezhab,"
                             "count(distinct case when day_open<=8 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01dezhac,"
                             "count(distinct case when day_open<=8 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01dezhaz,"
                             "count(distinct case when day_open<=8 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezhba,"
                             "count(distinct case when day_open<=8 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezhbb,"
                             "count(distinct case when day_open<=8 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01dezhbc,"
                             "count(distinct case when day_open<=8 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01dezhbz,"
                             "count(distinct case when day_open<=8 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezhza,"
                             "count(distinct case when day_open<=8 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezhzb,"
                             "count(distinct case when day_open<=8 and pay_result =1 then mec_no else null end) as t01dezhzc,"
                             "count(distinct case when day_open<=8 then mec_no else null end) as t01dezhzz "
                             "from personal_cfsl_loan_deduct_seq_countDayOpen_mid yy group by yy.idcard,yy.recall_date ")


countAllRDDS = sc.textFile(output_feature_path_pre + output_path + '/countAll')
#countAllRDDS = sc.textFile('/Users/xilin.zheng/yeepay/PRD4/data4/countAll')
countAllRDD = countAllRDDS.map(lambda x: x.split(',')).map(lambda row: (row[0], row[1], row[13]))
countAllDf = hsqlContext.createDataFrame(countAllRDD,
                                         ['idcard', 'recall_date', 't01dezzzz'])


cond = [countAllDf.idcard == countsqlDf.idcard, countAllDf.recall_date == countsqlDf.recall_date]
countAvgInitDf = countAllDf.join(countsqlDf, cond, 'left_outer').select(
    countAllDf.idcard, countAllDf.recall_date, countsqlDf.t01dezezz, countsqlDf.t01dezfzz, countsqlDf.t01dezgzz,
    countsqlDf.t01dezhzz,
    countAllDf.t01dezzzz)


hsqlContext.registerDataFrameAsTable(countAvgInitDf, "personal_cfsl_loan_deduct_seq_count")

countAvgDf = hsqlContext.sql("select idcard,recall_date,"
                             "round ( (case when t01dezezz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezezz/t01dezzzz end),2)  AS t02dezezz_dezzzz , "
                             "round ( (case when t01dezfzz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezfzz/t01dezzzz end),2)  AS t02dezfzz_dezzzz , "
                             "round ( (case when t01dezgzz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezgzz/t01dezzzz end),2)  AS t02dezgzz_dezzzz , "
                             "round ( (case when t01dezhzz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezhzz/t01dezzzz end),2)  AS t02dezhzz_dezzzz "
                             "from personal_cfsl_loan_deduct_seq_count")


keys = countsqlDf.rdd.map(lambda row: dropFrame(row))
keys.repartition(10).saveAsTextFile(output_feature_path_pre + output_path + '/' + key_cal)

keySeconds = countAvgDf.rdd.map(lambda row: dropFrame(row))
keySeconds.repartition(10).saveAsTextFile(output_feature_path_pre + output_path + '/' + 'countAvg_2')
#keySeconds.repartition(500).saveAsTextFile('/Users/xilin.zheng/yeepay/PRD4/data4/countAvg_2')

sc.stop()


print key_cal + "_sql_daily" + " success " + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))  +  "*"*90
