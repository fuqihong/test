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

key_cal = 'countDayOpen_1'
key_cal_pre = 'countAll'
print key_cal + "_sql_daily" + " run " + "*"*90

sc = SparkContext(appName= key_cal + "_sql_daily")



hsqlContext = HiveContext(sc)

countAllRDDS = sc.textFile(output_feature_hdfs_path + key_cal_pre)
countAllRDD = countAllRDDS.map(lambda x: x.split(',')).map(lambda row: (row[0], row[12]))
countAllDf = hsqlContext.createDataFrame(countAllRDD,
                                         ['idcard', 't01dezzzz'])



midsqlDf = hsqlContext.sql("select idcard,"
                           "no_mec as mec_no,"
                           "mec_type as goods_if_subbizcatname,"
                           "case when flag_error = 1 then 1 when flag_error > 1 then 2 else 3 end as req_if_trademsg,"
                           "pay_result as pay_result,"
                           "datediff('{current_time}', first_value(repay_tm) over(partition by no_mec,idcard order by repay_tm)) as day_open "
                           "from {mid_table}".format(current_time = yes_time, mid_table=input_mid_table_name))

hsqlContext.registerDataFrameAsTable(midsqlDf, "personal_cfsl_loan_deduct_seq_mid")

countDayPayMidDf = hsqlContext.sql("select distinct idcard,"
                                   "mec_no,"
                                   "goods_if_subbizcatname,"
                                   "req_if_trademsg,"
                                   "pay_result,"
                                   "case when day_open <= 1 then 1 when day_open <= 7 then 2 when day_open <= 14 then 3 when day_open <= 21 then 4 when day_open <= 30 then 5 when day_open <= 90 then 6 when day_open <= 180 then 7 when day_open <= 360 then 8 else 9 end as day_open "
                                   "from  personal_cfsl_loan_deduct_seq_mid where day_open <= 21 ")

hsqlContext.registerDataFrameAsTable(countDayPayMidDf, "personal_cfsl_loan_deduct_seq_countDayOpen_mid")

countsqlDf = hsqlContext.sql("select idcard,"
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
                             "from personal_cfsl_loan_deduct_seq_countDayOpen_mid yy group by yy.idcard")

#hsqlContext.registerDataFrameAsTable(countsqlDf, "personal_cfsl_loan_deduct_seq_count")

countAvgInitDf = countAllDf.join(countsqlDf, countAllDf.idcard == countsqlDf.idcard, 'left_outer').select(
    countAllDf.idcard, countsqlDf.t01dezazz, countsqlDf.t01dezbzz, countsqlDf.t01dezczz, countsqlDf.t01dezdzz,countAllDf.t01dezzzz)

hsqlContext.registerDataFrameAsTable(countAvgInitDf, "personal_cfsl_loan_deduct_seq_count")

countAvgDf = hsqlContext.sql("select idcard,"
                             "round ( (case when t01dezazz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezazz/t01dezzzz end),2)  AS t02dezazz_dezzzz , "
                             "round ( (case when t01dezbzz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezbzz/t01dezzzz end),2)  AS t02dezbzz_dezzzz , "
                             "round ( (case when t01dezczz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezczz/t01dezzzz end),2)  AS t02dezczz_dezzzz , "
                             "round ( (case when t01dezdzz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezdzz/t01dezzzz end),2)  AS t02dezdzz_dezzzz "
                             "from personal_cfsl_loan_deduct_seq_count")

save_path = output_feature_hdfs_path + key_cal
keys = countsqlDf.rdd.map(lambda row: dropFrame(row))
keys.repartition(500).saveAsTextFile(save_path)

save_path_1 = output_feature_hdfs_path + 'countAvg_1'
keySeconds = countAvgDf.rdd.map(lambda row: dropFrame(row))
keySeconds.repartition(500).saveAsTextFile(save_path_1)

sc.stop()
print key_cal + "_sql_daily" + " success " + "*"*90