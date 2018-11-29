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

key_cal = 'third_2'
sc = SparkContext(appName=key_cal + "_sql_daily")

hsqlContext = HiveContext(sc)


midDf = hsqlContext.sql("select * from {mid_table}".format(mid_table=input_mid_table_name))

hsqlContext.registerDataFrameAsTable(midDf, "personal_cfsl_loan_deduct_seq")

idcardDf = hsqlContext.sql("select distinct idcard from personal_cfsl_loan_deduct_seq")

hsqlContext.registerDataFrameAsTable(idcardDf, "idcardDf")

# 扣款失败后距离下一次成功的天数，按idcard
kksbNextDayAADf = hsqlContext.sql(
    "select aa.mec_type,aa.idcard,aa.repay_tm from personal_cfsl_loan_deduct_seq aa where aa.pay_result = 0")

hsqlContext.registerDataFrameAsTable(kksbNextDayAADf, "kksbNextDay_aa")

kksbNextDayBBInitDf = hsqlContext.sql(
    "select bb.idcard,bb.repay_tm,bb.pay_result,bb.mec_type,lag(bb.pay_result,1) over(partition by bb.idcard,bb.mec_type order by bb.repay_tm) as last_result from personal_cfsl_loan_deduct_seq bb")

hsqlContext.registerDataFrameAsTable(kksbNextDayBBInitDf, "kksbNextDay_bb_init")

kksbNextDayBBDf = hsqlContext.sql("select bb.idcard,"
                                  "bb.repay_tm,bb.mec_type,"
                                  "lag(bb.repay_tm,1,'2000-01-01') over(partition by bb.idcard,bb.mec_type order by bb.repay_tm) as last_repay_tm "
                                  "from kksbNextDay_bb_init bb where bb.pay_result = 1 and bb.last_result = 0")

hsqlContext.registerDataFrameAsTable(kksbNextDayBBDf, "kksbNextDay_bb")

kksbNextDayDf = hsqlContext.sql("select aa.idcard,"
                                "max(case when aa.mec_type = 'cf' then datediff(bb.repay_tm,aa.repay_tm) else null end) as t03td136,"
                                "max(case when aa.mec_type = 'sl' then datediff(bb.repay_tm,aa.repay_tm) else null end )as t03td137,"
                                "min(case when aa.mec_type = 'cf' then datediff(bb.repay_tm,aa.repay_tm) else null end) as t03td139,"
                                "min(case when aa.mec_type = 'sl' then datediff(bb.repay_tm,aa.repay_tm) else null end )as t03td140,"
                                "round(avg(case when aa.mec_type = 'cf' then datediff(bb.repay_tm,aa.repay_tm) else null end),2) as t03td142,"
                                "round(avg(case when aa.mec_type = 'sl' then datediff(bb.repay_tm,aa.repay_tm) else null end ),2)as t03td143 "
                                "from kksbNextDay_aa aa join kksbNextDay_bb bb on aa.idcard = bb.idcard and aa.mec_type = bb.mec_type "
                                "where aa.repay_tm > bb.last_repay_tm and aa.repay_tm < bb.repay_tm group by aa.idcard")

hsqlContext.registerDataFrameAsTable(kksbNextDayDf, "kksbNextDayDf_table")
third_1_df = idcardDf.join(kksbNextDayDf, idcardDf.idcard == kksbNextDayDf.idcard, 'left_outer').select(
    idcardDf.idcard,
    kksbNextDayDf.t03td136, kksbNextDayDf.t03td137, kksbNextDayDf.t03td139,
    kksbNextDayDf.t03td140, kksbNextDayDf.t03td142, kksbNextDayDf.t03td143)

#third_1_df.show()
#third_1_df = hsqlContext.sql("select bb.idcard,aa.t03td136, aa.t03td137, aa.t03td139,aa.t03td140,aa.t03td142,aa.t03td143 from idcardDf bb left join kksbNextDayDf_table aa on bb.idcard = aa.idcard")

#扣款失败后距离下一次成功的天数，按idcard
kksbNextDayAADf2 = hsqlContext.sql(
    "select aa.idcard,aa.repay_tm from personal_cfsl_loan_deduct_seq aa where aa.pay_result = 0")

hsqlContext.registerDataFrameAsTable(kksbNextDayAADf2, "kksbNextDay_cc")

kksbNextDayBBInitDf2 = hsqlContext.sql(
    "select bb.idcard,bb.repay_tm,bb.pay_result,lag(bb.pay_result,1) over(partition by bb.idcard order by bb.repay_tm) as last_result from personal_cfsl_loan_deduct_seq bb")

hsqlContext.registerDataFrameAsTable(kksbNextDayBBInitDf2, "kksbNextDay_dd_init")

kksbNextDayBBDf2 = hsqlContext.sql("select bb.idcard,"
                                   "bb.repay_tm,"
                                   "lag(bb.repay_tm,1,'2000-01-01') over(partition by bb.idcard order by bb.repay_tm) as last_repay_tm "
                                   "from kksbNextDay_dd_init bb where bb.pay_result = 1 and bb.last_result = 0")

hsqlContext.registerDataFrameAsTable(kksbNextDayBBDf2, "kksbNextDay_dd")

kksbNextDayDf2 = hsqlContext.sql("select aa.idcard,"
                                 "max(datediff(bb.repay_tm,aa.repay_tm)) as t03td138,"
                                 "min(datediff(bb.repay_tm,aa.repay_tm)) as t03td141,"
                                 "round(avg(datediff(bb.repay_tm,aa.repay_tm)),2) as t03td144 "
                                 "from kksbNextDay_cc aa join kksbNextDay_dd bb on aa.idcard = bb.idcard "
                                 "where aa.repay_tm > bb.last_repay_tm and aa.repay_tm < bb.repay_tm group by aa.idcard")


third_2_df = third_1_df.join(kksbNextDayDf2, third_1_df.idcard == kksbNextDayDf2.idcard, 'left_outer').select(
    third_1_df.idcard,
    third_1_df.t03td136, third_1_df.t03td137, kksbNextDayDf2.t03td138, third_1_df.t03td139,
    third_1_df.t03td140, kksbNextDayDf2.t03td141, third_1_df.t03td142, third_1_df.t03td143,
    kksbNextDayDf2.t03td144)

#third_2_df.show()

# 最后一次扣款因为余额不足失败后扣款次数求和
zhyckkCountInitDf = hsqlContext.sql("select idcard,"
                                    "repay_tm,"
                                    "mec_type,"
                                    "pay_result,"
                                    "first_value(repay_tm) over(partition by idcard,mec_type order by case when pay_result = 1 then cast(date_sub(repay_tm,5475)  as timestamp) else repay_tm end desc) as last_fail_tm "
                                    "from personal_cfsl_loan_deduct_seq")

hsqlContext.registerDataFrameAsTable(zhyckkCountInitDf, "zhyckkCount_init")

zhyckkCountDf = hsqlContext.sql("select idcard,"
                                "sum(case when aa.mec_type = 'cf' then 1 else 0 end) as t03td145,"
                                "sum(case when aa.mec_type = 'sl' then 1 else 0 end) as t03td146 "
                                "from zhyckkCount_init aa where aa.pay_result = 1 and aa.repay_tm > last_fail_tm group by idcard")

third_3_df = third_2_df.join(zhyckkCountDf, third_2_df.idcard == zhyckkCountDf.idcard, 'left_outer').select(
    third_2_df.idcard,
    third_2_df.t03td136, third_2_df.t03td137, third_2_df.t03td138, third_2_df.t03td139,
    third_2_df.t03td140, third_2_df.t03td141, third_2_df.t03td142, third_2_df.t03td143,
    third_2_df.t03td144, zhyckkCountDf.t03td145, zhyckkCountDf.t03td146)

#third_3_df.show()

zhyckkCountInitDf2 = hsqlContext.sql("select idcard,"
                                     "repay_tm,"
                                     "pay_result,"
                                     "first_value(repay_tm) over(partition by idcard order by case when pay_result = 1 then cast(date_sub(repay_tm,5475)  as timestamp) else repay_tm end desc) as last_fail_tm "
                                     "from personal_cfsl_loan_deduct_seq")

hsqlContext.registerDataFrameAsTable(zhyckkCountInitDf2, "zhyckkCount2_init")

zhyckkCountDf2 = hsqlContext.sql("select idcard,"
                                 "count(*) as t03td147 "
                                 "from zhyckkCount2_init aa where aa.pay_result = 1 and aa.repay_tm > last_fail_tm group by idcard")

third_4_df = third_3_df.join(zhyckkCountDf2, third_3_df.idcard == zhyckkCountDf2.idcard, 'left_outer').select(
    third_3_df.idcard,
    third_3_df.t03td136, third_3_df.t03td137, third_3_df.t03td138, third_3_df.t03td139,
    third_3_df.t03td140, third_3_df.t03td141, third_3_df.t03td142, third_3_df.t03td143,
    third_3_df.t03td144, third_3_df.t03td145, third_3_df.t03td146, zhyckkCountDf2.t03td147)

#third_4_df.show()
#
# # 当前逾期机构数 最近一次扣款为扣款失败机构数
# # 当前履约机构数 最近一次扣款为扣款成功机构数
#
zjyckkOrgCountInitDf = hsqlContext.sql("select idcard, "
                                       "no_mec,"
                                       "pay_result,"
                                       "row_number() over(partition by idcard,no_mec order by repay_tm desc) rn "
                                       "from personal_cfsl_loan_deduct_seq aa ")

hsqlContext.registerDataFrameAsTable(zjyckkOrgCountInitDf, "zjyckkOrgCount_init")

zjyckkOrgCountDf = hsqlContext.sql("select aa.idcard,"
                                   "count(distinct case when aa.pay_result = 0 then no_mec else null end) as t03td148,"
                                   "count(distinct case when aa.pay_result = 1 then no_mec else null end) as t03td149 "
                                   "from  zjyckkOrgCount_init aa where rn = 1 group by aa.idcard")

third_5_df = third_4_df.join(zjyckkOrgCountDf, third_4_df.idcard == zjyckkOrgCountDf.idcard, 'left_outer').select(
    third_4_df.idcard,
    third_4_df.t03td136, third_4_df.t03td137, third_4_df.t03td138, third_4_df.t03td139,
    third_4_df.t03td140, third_4_df.t03td141, third_4_df.t03td142, third_4_df.t03td143,
    third_4_df.t03td144, third_4_df.t03td145, third_4_df.t03td146, third_4_df.t03td147, zjyckkOrgCountDf.t03td148,
    zjyckkOrgCountDf.t03td149)
#third_5_df.show()
#
# # 睡眠机构数 截止查询时间，用户6个月内无交易记录的机构数
t03td150MidDf = hsqlContext.sql("select idcard as idcard, "
                                "no_mec as merchants,"
                                "max(repay_tm) as last_pay_tm "
                                "from personal_cfsl_loan_deduct_seq group by idcard,no_mec")

hsqlContext.registerDataFrameAsTable(t03td150MidDf, "t03td150_mid")
# 睡眠机构数 t03td150
t03td150Df = hsqlContext.sql("select med.idcard,"
                             "count(distinct case when datediff(current_date(),last_pay_tm)>180 then merchants else null end) as t03td150 "
                             "from t03td150_mid as med group by med.idcard")

third_6_df = third_5_df.join(t03td150Df, third_5_df.idcard == t03td150Df.idcard, 'left_outer').select(
    third_5_df.idcard,
    third_5_df.t03td136, third_5_df.t03td137, third_5_df.t03td138, third_5_df.t03td139,
    third_5_df.t03td140, third_5_df.t03td141, third_5_df.t03td142, third_5_df.t03td143,
    third_5_df.t03td144, third_5_df.t03td145, third_5_df.t03td146, third_5_df.t03td147, third_5_df.t03td148,
    third_5_df.t03td149, t03td150Df.t03td150)

#third_6_df.show()
# # 最近1次 扣款失败(-客户余额不足) 金额
# # 扣款失败(-除余额不足外其他客观原因)
# # 扣款成功
# # 全
zjyckkAmountMidDf = hsqlContext.sql("select idcard,"
                                    "amt,"
                                    "case when pay_result = 1 then '1' "
                                    "when (pay_result = 0 and flag_error = '1') then '0' "
                                    "when (pay_result = 0 and flag_error regexp '2|3|4|5') then '-1'  else null end as trademsg,"
                                    "row_number() over(partition by idcard order by repay_tm desc) as pay_order "
                                    "from personal_cfsl_loan_deduct_seq")

hsqlContext.registerDataFrameAsTable(zjyckkAmountMidDf, "zjyckkAmount_mid")
# 最近1次扣款金额表
zjyckkAmountDf = hsqlContext.sql("select ot.idcard,"
                                 "round(case when ot.trademsg = '0' then ot.amt else null end,2) as t03td151,"
                                 "round(case when ot.trademsg = '-1' then ot.amt else null end,2) as t03td152,"
                                 "round(case when ot.trademsg = '1' then ot.amt else null end,2) as t03td153,"
                                 "round(ot.amt,2) as t03td154 from zjyckkAmount_mid ot where ot.pay_order=1")

third_7_df = third_6_df.join(zjyckkAmountDf, third_6_df.idcard == zjyckkAmountDf.idcard, 'left_outer').select(
    third_6_df.idcard,
    third_6_df.t03td136, third_6_df.t03td137, third_6_df.t03td138, third_6_df.t03td139,
    third_6_df.t03td140, third_6_df.t03td141, third_6_df.t03td142, third_6_df.t03td143,
    third_6_df.t03td144, third_6_df.t03td145, third_6_df.t03td146, third_6_df.t03td147, third_6_df.t03td148,
    third_6_df.t03td149, third_6_df.t03td150, zjyckkAmountDf.t03td151, zjyckkAmountDf.t03td152, zjyckkAmountDf.t03td153,
    zjyckkAmountDf.t03td154)
#third_7_df.show()

save_path = output_feature_hdfs_path + key_cal
keys = third_7_df.rdd.map(lambda row: dropFrame(row))
keys.repartition(500).saveAsTextFile(save_path)

sc.stop()
