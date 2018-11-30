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

key_cal = 'countRowNum'
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
                           "row_number() over (partition by aa.idcard,aa.recall_date order by repay_tm desc ) as row_num "
                           "from personal_cfsl_loan_deduct_seq aa ")

hsqlContext.registerDataFrameAsTable(midsqlDf, "personal_cfsl_loan_deduct_seq_mid")

countRowNumMidDf = hsqlContext.sql("select distinct idcard,recall_date,"
                                    "mec_no,"
                                    "goods_if_subbizcatname,"
                                    "req_if_trademsg,"
                                    "pay_result,"
                                    "case when row_num <= 5 then 1 when row_num <= 20 then 2 when row_num <= 50 then 3 when row_num <= 100 then 4 else 5 end as row_num "
                                    "from  personal_cfsl_loan_deduct_seq_mid where row_num <= 100")

hsqlContext.registerDataFrameAsTable(countRowNumMidDf, "personal_cfsl_loan_deduct_seq_countRowNum_mid")

countsqlDf = hsqlContext.sql("select idcard,recall_date,"
                             "count(distinct case when row_num<=1 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01deizaa,"
                             "count(distinct case when row_num<=1 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01deizab,"
                             "count(distinct case when row_num<=1 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01deizac,"
                             "count(distinct case when row_num<=1 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01deizaz,"
                             "count(distinct case when row_num<=1 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01deizba,"
                             "count(distinct case when row_num<=1 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01deizbb,"
                             "count(distinct case when row_num<=1 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01deizbc,"
                             "count(distinct case when row_num<=1 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01deizbz,"
                             "count(distinct case when row_num<=1 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01deizza,"
                             "count(distinct case when row_num<=1 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01deizzb,"
                             "count(distinct case when row_num<=1 and pay_result =1 then mec_no else null end) as t01deizzc,"
                             "count(distinct case when row_num<=1 then mec_no else null end) as t01deizzz,"
                             "count(distinct case when row_num<=2 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dejzaa,"
                             "count(distinct case when row_num<=2 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dejzab,"
                             "count(distinct case when row_num<=2 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01dejzac,"
                             "count(distinct case when row_num<=2 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01dejzaz,"
                             "count(distinct case when row_num<=2 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dejzba,"
                             "count(distinct case when row_num<=2 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dejzbb,"
                             "count(distinct case when row_num<=2 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01dejzbc,"
                             "count(distinct case when row_num<=2 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01dejzbz,"
                             "count(distinct case when row_num<=2 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dejzza,"
                             "count(distinct case when row_num<=2 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dejzzb,"
                             "count(distinct case when row_num<=2 and pay_result =1 then mec_no else null end) as t01dejzzc,"
                             "count(distinct case when row_num<=2 then mec_no else null end) as t01dejzzz,"
                             "count(distinct case when row_num<=3 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dekzaa,"
                             "count(distinct case when row_num<=3 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dekzab,"
                             "count(distinct case when row_num<=3 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01dekzac,"
                             "count(distinct case when row_num<=3 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01dekzaz,"
                             "count(distinct case when row_num<=3 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dekzba,"
                             "count(distinct case when row_num<=3 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dekzbb,"
                             "count(distinct case when row_num<=3 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01dekzbc,"
                             "count(distinct case when row_num<=3 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01dekzbz,"
                             "count(distinct case when row_num<=3 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01dekzza,"
                             "count(distinct case when row_num<=3 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01dekzzb,"
                             "count(distinct case when row_num<=3 and pay_result =1 then mec_no else null end) as t01dekzzc,"
                             "count(distinct case when row_num<=3 then mec_no else null end) as t01dekzzz,"
                             "count(distinct case when row_num<=4 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01delzaa,"
                             "count(distinct case when row_num<=4 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01delzab,"
                             "count(distinct case when row_num<=4 and goods_if_subbizcatname = 'cf' and pay_result =1 then mec_no else null end) as t01delzac,"
                             "count(distinct case when row_num<=4 and goods_if_subbizcatname = 'cf' then mec_no else null end) as t01delzaz,"
                             "count(distinct case when row_num<=4 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01delzba,"
                             "count(distinct case when row_num<=4 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01delzbb,"
                             "count(distinct case when row_num<=4 and goods_if_subbizcatname ='sl' and pay_result =1 then mec_no else null end) as t01delzbc,"
                             "count(distinct case when row_num<=4 and goods_if_subbizcatname ='sl' then mec_no else null end) as t01delzbz,"
                             "count(distinct case when row_num<=4 and pay_result =0 and req_if_trademsg = 1 then mec_no else null end) as t01delzza,"
                             "count(distinct case when row_num<=4 and pay_result =0 and req_if_trademsg = 2 then mec_no else null end) as t01delzzb,"
                             "count(distinct case when row_num<=4 and pay_result =1 then mec_no else null end) as t01delzzc,"
                             "count(distinct case when row_num<=4 then mec_no else null end) as t01delzzz "
                             "from personal_cfsl_loan_deduct_seq_countRowNum_mid yy group by yy.idcard,yy.recall_date")

keys = countsqlDf.rdd.map(lambda row: dropFrame(row))

keys.repartition(100).saveAsTextFile(output_feature_path_pre + output_path + '/' + key_cal)
sc.stop()

print key_cal + "_sql_daily" + " success " + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))  +  "*"*90
