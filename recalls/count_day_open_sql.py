# encoding:utf-8 
count_day_open_1_sql="""
sum(case when day_open<=3 then 1 else 0 end) as t01dezczz,
sum(case when day_open<=4 then 1 else 0 end) as t01dezdzz,
sum(case when day_open<=5 then 1 else 0 end) as t01dezezz,
sum(case when day_open<=6 then 1 else 0 end) as t01dezfzz,
sum(case when day_open<=7 then 1 else 0 end) as t01dezgzz,
sum(case when day_open<=8 then 1 else 0 end) as t01dezhzz

"""
count_day_open_2_1_sql="""
sum(case when day_open<=4 and goods_if_subbizcatname ='sl' then 1 else 0 end) as t01dezdbz,
sum(case when day_open<=5 and goods_if_subbizcatname ='sl' then 1 else 0 end) as t01dezebz,
sum(case when day_open<=6 and goods_if_subbizcatname = 'cf' then 1 else 0 end) as t01dezfaz,
sum(case when day_open<=6 and goods_if_subbizcatname ='sl' then 1 else 0 end) as t01dezfbz,
sum(case when day_open<=7 and goods_if_subbizcatname = 'cf' then 1 else 0 end) as t01dezgaz,
sum(case when day_open<=7 and goods_if_subbizcatname ='sl' then 1 else 0 end) as t01dezgbz,
sum(case when day_open<=8 and goods_if_subbizcatname = 'cf' then 1 else 0 end) as t01dezhaz,
sum(case when day_open<=8 and goods_if_subbizcatname ='sl' then 1 else 0 end) as t01dezhbz

"""
count_day_open_2_2_sql="""
sum(case when day_open<=4 and pay_result =1 then 1 else 0 end) as t01dezdzc,
sum(case when day_open<=5 and pay_result =1 then 1 else 0 end) as t01dezezc,
sum(case when day_open<=6 and pay_result =1 then 1 else 0 end) as t01dezfzc,
sum(case when day_open<=7 and pay_result =1 then 1 else 0 end) as t01dezgzc,
sum(case when day_open<=8 and pay_result =1 then 1 else 0 end) as t01dezhzc

"""
count_day_open_3_1_sql="""
sum(case when day_open<=4 and goods_if_subbizcatname ='sl' and pay_result =1 then 1 else 0 end) as t01dezdbc,
sum(case when day_open<=5 and goods_if_subbizcatname ='sl' and pay_result =1 then 1 else 0 end) as t01dezebc,
sum(case when day_open<=6 and goods_if_subbizcatname = 'cf' and pay_result =1 then 1 else 0 end) as t01dezfac,
sum(case when day_open<=6 and goods_if_subbizcatname ='sl' and pay_result =1 then 1 else 0 end) as t01dezfbc,
sum(case when day_open<=7 and goods_if_subbizcatname = 'cf' and pay_result =1 then 1 else 0 end) as t01dezgac,
sum(case when day_open<=7 and goods_if_subbizcatname ='sl' and pay_result =1 then 1 else 0 end) as t01dezgbc,
sum(case when day_open<=8 and goods_if_subbizcatname = 'cf' and pay_result =1 then 1 else 0 end) as t01dezhac,
sum(case when day_open<=8 and goods_if_subbizcatname ='sl' and pay_result =1 then 1 else 0 end) as t01dezhbc

"""
count_day_open_3_2_sql="""
sum(case when day_open<=5 and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dezeza,
sum(case when day_open<=6 and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dezfza,
sum(case when day_open<=7 and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dezgza,
sum(case when day_open<=8 and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dezhza,
sum(case when day_open<=8 and pay_result =0 and req_if_trademsg = 2 then 1 else 0 end) as t01dezhzb

"""
count_day_open_4_sql="""
sum(case when day_open<=6 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dezfba,
sum(case when day_open<=7 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dezgaa,
sum(case when day_open<=7 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dezgba,
sum(case when day_open<=8 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dezhaa,
sum(case when day_open<=8 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dezhba,

"""
count_day_open_avg_sql="""
round ( (case when t01dezczz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezczz/t01dezzzz end),2)  AS t02dezczz_dezzzz ,
round ( (case when t01dezdzz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezdzz/t01dezzzz end),2)  AS t02dezdzz_dezzzz ,
round ( (case when t01dezezz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezezz/t01dezzzz end),2)  AS t02dezezz_dezzzz ,
round ( (case when t01dezfzz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezfzz/t01dezzzz end),2)  AS t02dezfzz_dezzzz ,
round ( (case when t01dezgzz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezgzz/t01dezzzz end),2)  AS t02dezgzz_dezzzz ,
round ( (case when t01dezhzz IS NULL THEN NULL when t01dezzzz IS NULL THEN NULL when t01dezzzz=0 THEN 0 ELSE t01dezhzz/t01dezzzz end),2)  AS t02dezhzz_dezzzz 

"""
