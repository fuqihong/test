# encoding:utf-8 
third_count_1_sql="""
sum(case when aa.mec_type = 'sl' and aa.day_pay <= 2 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td013,
sum(case when aa.mec_type = 'sl' and aa.day_pay <= 3 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td021,
sum(case when aa.mec_type = 'cf' and aa.day_pay <= 4 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td025,
sum(case when aa.mec_type = 'sl' and aa.day_pay <= 4 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td029,
sum(case when aa.mec_type = 'cf' and aa.day_pay <= 5 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td033,
sum(case when aa.mec_type = 'sl' and aa.day_pay <= 5 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td037,
sum(case when aa.mec_type = 'sl' and aa.day_pay <= 2 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td068,
sum(case when aa.mec_type = 'sl' and aa.day_pay <= 3 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td070,
sum(case when aa.mec_type = 'cf' and aa.day_pay <= 4 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td071,
sum(case when aa.mec_type = 'sl' and aa.day_pay <= 4 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td072,
sum(case when aa.mec_type = 'cf' and aa.day_pay <= 5 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td073,
sum(case when aa.mec_type = 'sl' and aa.day_pay <= 5 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td074,
sum( case when aa.mec_type = 'cf' and aa.day_pay <= 6 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td041,
sum( case when aa.mec_type = 'sl' and aa.day_pay <= 6 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td045,
sum( case when aa.mec_type = 'cf' and aa.day_pay <= 7 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td049,
sum( case when aa.mec_type = 'sl' and aa.day_pay <= 7 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td053,
sum( case when aa.mec_type = 'cf' and aa.day_pay <= 8 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td057,
sum( case when aa.mec_type = 'sl' and aa.day_pay <= 8 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td061,
sum( case when aa.mec_type = 'cf' and aa.day_pay <= 6 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td075,
sum( case when aa.mec_type = 'sl' and aa.day_pay <= 6 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td076,
sum( case when aa.mec_type = 'cf' and aa.day_pay <= 7 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td077,
sum( case when aa.mec_type = 'sl' and aa.day_pay <= 7 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td078,
sum( case when aa.mec_type = 'cf' and aa.day_pay <= 8 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td079,
sum( case when aa.mec_type = 'sl' and aa.day_pay <= 8 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td080

"""
third_count_2_sql="""
sum( case when aa.mec_type = 'cf' and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td081,
sum( case when aa.mec_type = 'sl' and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td085,
sum( case when aa.mec_type = 'cf' and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td089,
sum( case when aa.mec_type = 'sl' and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td090

"""
third_count_3_sql="""
sum( case when aa.day_pay <= 2 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td095,
sum( case when aa.day_pay <= 3 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td099,
sum( case when aa.day_pay <= 4 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td103,
sum( case when aa.day_pay <= 5 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td107,
sum( case when aa.day_pay <= 6 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td111,
sum( case when aa.day_pay <= 7 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td115,
sum( case when aa.day_pay <= 8 and aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td119,
sum( case when aa.day_pay <= 2 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td124,
sum( case when aa.day_pay <= 3 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td125,
sum( case when aa.day_pay <= 4 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td126,
sum( case when aa.day_pay <= 5 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td127,
sum( case when aa.day_pay <= 6 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td128,
sum( case when aa.day_pay <= 7 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td129,
sum( case when aa.day_pay <= 8 and aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td130

"""
third_count_4_sql="""
sum( case when aa.pay_result = 0 and aa.times >= 2 then 1 else 0 end) as t03td131,
sum( case when aa.pay_result = 1 and aa.times >= 2 then 1 else 0 end) as t03td135 

"""
