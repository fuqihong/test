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

key_cal = 'countDayOpen_1'
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
                                   "case when day_open <= 1 then 1 when day_open <= 7 then 2 when day_open <= 14 then 3 when day_open <= 21 then 4 when day_open <= 30 then 5 when day_open <= 90 then 6 when day_open <= 180 then 7 when day_open <= 360 then 8 else 9 end as day_open "
                                   "from  personal_cfsl_loan_deduct_seq_mid where day_open <= 21 ")

hsqlContext.registerDataFrameAsTable(countDayPayMidDf, "personal_cfsl_loan_deduct_seq_countDayOpen_mid")

countsqlDf = hsqlContext.sql("select idcard,recall_date,"
                             "count(distinct case when day_open<=1 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezaaa,"
                             "count(distinct case when day_open<=1 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezaab,"
                             "count(distinct case when day_open<=1 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01dezaac,"
                             "count(distinct case when day_open<=1 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01dezaaz,"
                             "count(distinct case when day_open<=1 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezaba,"
                             "count(distinct case when day_open<=1 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezabb,"
                             "count(distinct case when day_open<=1 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01dezabc,"
                             "count(distinct case when day_open<=1 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01dezabz,"
                             "count(distinct case when day_open<=1 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezaza,"
                             "count(distinct case when day_open<=1 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezazb,"
                             "count(distinct case when day_open<=1 and pay_result =1 then mec_no else null end) as t01dezazc,"
                             "count(distinct case when day_open<=1 then mec_no else null end) as t01dezazz,"
                             "count(distinct case when day_open<=2 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezbaa,"
                             "count(distinct case when day_open<=2 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezbab,"
                             "count(distinct case when day_open<=2 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01dezbac,"
                             "count(distinct case when day_open<=2 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01dezbaz,"
                             "count(distinct case when day_open<=2 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezbba,"
                             "count(distinct case when day_open<=2 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezbbb,"
                             "count(distinct case when day_open<=2 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01dezbbc,"
                             "count(distinct case when day_open<=2 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01dezbbz,"
                             "count(distinct case when day_open<=2 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezbza,"
                             "count(distinct case when day_open<=2 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezbzb,"
                             "count(distinct case when day_open<=2 and pay_result =1 then mec_no else null end) as t01dezbzc,"
                             "count(distinct case when day_open<=2 then mec_no else null end) as t01dezbzz,"
                             "count(distinct case when day_open<=3 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezcaa,"
                             "count(distinct case when day_open<=3 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezcab,"
                             "count(distinct case when day_open<=3 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01dezcac,"
                             "count(distinct case when day_open<=3 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01dezcaz,"
                             "count(distinct case when day_open<=3 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezcba,"
                             "count(distinct case when day_open<=3 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezcbb,"
                             "count(distinct case when day_open<=3 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01dezcbc,"
                             "count(distinct case when day_open<=3 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01dezcbz,"
                             "count(distinct case when day_open<=3 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezcza,"
                             "count(distinct case when day_open<=3 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezczb,"
                             "count(distinct case when day_open<=3 and pay_result =1 then mec_no else null end) as t01dezczc,"
                             "count(distinct case when day_open<=3 then mec_no else null end) as t01dezczz,"
                             "count(distinct case when day_open<=4 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezdaa,"
                             "count(distinct case when day_open<=4 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezdab,"
                             "count(distinct case when day_open<=4 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01dezdac,"
                             "count(distinct case when day_open<=4 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01dezdaz,"
                             "count(distinct case when day_open<=4 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezdba,"
                             "count(distinct case when day_open<=4 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezdbb,"
                             "count(distinct case when day_open<=4 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01dezdbc,"
                             "count(distinct case when day_open<=4 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01dezdbz,"
                             "count(distinct case when day_open<=4 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dezdza,"
                             "count(distinct case when day_open<=4 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dezdzb,"
                             "count(distinct case when day_open<=4 and pay_result =1 then mec_no else null end) as t01dezdzc,"
                             "count(distinct case when day_open<=4 then mec_no else null end) as t01dezdzz "
                             "from personal_cfsl_loan_deduct_seq_countDayOpen_mid yy group by yy.idcard,yy.recall_date")

countAllRDDS = sc.textFile(output_feature_path_pre + output_path + 'countAll')
#countAllRDDS = sc.textFile('/Users/xilin.zheng/yeepay/PRD4/data4/countAll')
countAllRDD = countAllRDDS.map(lambda x: x.split(',')).map(lambda row: (row[0], row[1], row[13]))
countAllDf = hsqlContext.createDataFrame(countAllRDD,
                                         ['idcard', 'recall_date', 't01dezzzz'])


cond = [countAllDf.idcard == countsqlDf.idcard, countAllDf.recall_date == countsqlDf.recall_date]
countAvgInitDf = countAllDf.join(countsqlDf, cond, 'left_outer').select(
    countAllDf.idcard, countAllDf.recall_date, countsqlDf.t01dezazz, countsqlDf.t01dezbzz, countsqlDf.t01dezczz,
    countsqlDf.t01dezdzz,
    countAllDf.t01dezzzz)


hsqlContext.registerDataFrameAsTable(countAvgInitDf, "personal_cfsl_loan_deduct_seq_count")

countAvgDf = hsqlContext.sql("select idcard,recall_date,"
                             "round ( (case when t01dezazz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezazz/t01dezzzz end),2)  AS t02dezazz_dezzzz , "
                             "round ( (case when t01dezbzz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezbzz/t01dezzzz end),2)  AS t02dezbzz_dezzzz , "
                             "round ( (case when t01dezczz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezczz/t01dezzzz end),2)  AS t02dezczz_dezzzz , "
                             "round ( (case when t01dezdzz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezdzz/t01dezzzz end),2)  AS t02dezdzz_dezzzz "
                             "from personal_cfsl_loan_deduct_seq_count")

keys = countsqlDf.rdd.map(lambda row: dropFrame(row))
keys.repartition(10).saveAsTextFile(output_feature_path_pre + output_path + '/' + key_cal)

keySeconds = countAvgDf.rdd.map(lambda row: dropFrame(row))
keySeconds.repartition(10).saveAsTextFile(output_feature_path_pre + output_path + '/' + 'countAvg_1')

sc.stop()

print key_cal + "_sql_daily" + " success " + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))  +  "*"*90
