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

key_cal_pre = 'countAll'
key_cal = 'countDayOpen_2'
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
                           "datediff(current_timestamp, first_value(repay_tm) over(partition by no_mec,idcard order by repay_tm)) as day_open "
                           "from {mid_table}".format(mid_table=input_mid_table_name))

hsqlContext.registerDataFrameAsTable(midsqlDf, "personal_cfsl_loan_deduct_seq_mid")

countDayPayMidDf = hsqlContext.sql("select distinct idcard,"
                                   "mec_no,"
                                   "goods_if_subbizcatname,"
                                   "req_if_trademsg,"
                                   "pay_result,"
                                   "case when day_open <= 30 then 5 when day_open <= 90 then 6 when day_open <= 180 then 7 when day_open <= 360 then 8 else 9 end as day_open "
                                   "from personal_cfsl_loan_deduct_seq_mid where day_open <= 360 ")

hsqlContext.registerDataFrameAsTable(countDayPayMidDf, "personal_cfsl_loan_deduct_seq_countDayOpen_mid")

countsqlDf = hsqlContext.sql("select idcard,"
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
                             "from personal_cfsl_loan_deduct_seq_countDayOpen_mid yy group by yy.idcard")

#hsqlContext.registerDataFrameAsTable(countsqlDf, "personal_cfsl_loan_deduct_seq_count")

countAvgInitDf = countAllDf.join(countsqlDf, countAllDf.idcard == countsqlDf.idcard, 'left_outer').select(
    countAllDf.idcard, countsqlDf.t01dezezz, countsqlDf.t01dezfzz, countsqlDf.t01dezgzz, countsqlDf.t01dezhzz,countAllDf.t01dezzzz)

hsqlContext.registerDataFrameAsTable(countAvgInitDf, "personal_cfsl_loan_deduct_seq_count")

countAvgDf = hsqlContext.sql("select idcard,"
                             "round ( (case when t01dezezz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezezz/t01dezzzz end),2)  AS t02dezezz_dezzzz , "
                             "round ( (case when t01dezfzz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezfzz/t01dezzzz end),2)  AS t02dezfzz_dezzzz , "
                             "round ( (case when t01dezgzz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezgzz/t01dezzzz end),2)  AS t02dezgzz_dezzzz , "
                             "round ( (case when t01dezhzz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezhzz/t01dezzzz end),2)  AS t02dezhzz_dezzzz "
                             "from personal_cfsl_loan_deduct_seq_count")

save_path = output_feature_hdfs_path + key_cal
keys = countsqlDf.rdd.map(lambda row: dropFrame(row))
keys.repartition(500).saveAsTextFile(save_path)

save_path_2 = output_feature_hdfs_path + 'countAvg_2'
keySeconds = countAvgDf.rdd.map(lambda row: dropFrame(row))
keySeconds.repartition(500).saveAsTextFile(save_path_2)


sc.stop()
